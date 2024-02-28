from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
import warnings
import inspect
import numpy as np
from multiprocess import cpu_count, Process, Pipe, Queue
import copy
from tqdm import tqdm
from itertools import chain
import os
import re
import threading

class ProcessNode(ABC):
    """
    The ProcessNode class describes the interface of single processing unit, here called node.
    Nodes take inputs and produce outputs from those inputs.

    Argument:
    ---------
    name: Unique name for node within the workflow it recides.
    cache_data: Whether or not to store the produced outputs in cache. Default 'False'.

    Attributes:
    -----------
    name: 
        Name of node instance
    inputs: 
        All inputs to the node
    mandatory_inputs: 
        The mandatory inputs to the node
    non_mandatory_inputs: 
        The non-mandatory inputs to the node
    default_inputs: 
        The default values used for the non-mandatory inputs
    outputs: 
        The outputs produced by the node, needs to be provided by each subclass of ProcessNode
    run: 
        Generic run method which check inputs, calles _run function and checks its outputs
    _run: 
        Node specific run function, needs to be provided by each subclass of ProcessNode
    """
    def __init__(self, description : str = "", create_input_output_attr : bool = True) -> None:
        self._description = description
        self._cache_input = False
        self._cache_output = False
        self._input_cache = None
        self._output_cache = None
        if create_input_output_attr:
            self._createInputOutputAttributes()

    @property
    def inputs(self) -> tuple:
        return tuple(inspect.getfullargspec(self._run).args[1:])
    
    @property
    def mandatory_inputs(self) -> tuple:
        return self.inputs[:len(self.inputs)-len(self.default_inputs)]
    
    @property
    def default_inputs(self) -> tuple:
        if self._run.__defaults__ is None:
            def_args = tuple()
        else:
            def_args = self._run.__defaults__
        return def_args
    
    @property
    def non_mandatory_inputs(self) -> tuple:
        return self.inputs[len(self.inputs)-len(self.default_inputs):]
    
    @property
    def description(self) -> str:
        return self._description
    
    @property
    def has_cache_ignore_option(self):
        return "ignore_cache" in inspect.getfullargspec(self._run).args
    
    @property
    def has_verbose_option(self):
        return "verbose" in inspect.getfullargspec(self._run).args
    
    @description.setter
    def description(self, val : str) -> None:
        self._description = str(val)

    @property
    def cache_data(self) -> bool:
        return self._cache_output and self._cache_input
    
    @cache_data.setter
    def cache_data(self, val : bool) -> None:
        self.cache_output = bool(val)
        self.cache_input = bool(val)

    @property
    def cache_input(self) -> bool:
        return self._cache_input
    
    @cache_input.setter
    def cache_input(self, val : bool) -> None:
        self._cache_input = bool(val)
        if not self._cache_input:
            self._input_cache = None

    @property
    def cache_output(self) -> bool:
        return self._cache_output
    
    @cache_output.setter
    def cache_output(self, val : bool) -> None:
        self._cache_output = bool(val)
        if not self._cache_output:
            self._output_cache = None

    @property
    def input_cache(self) -> dict:
        if self._input_cache is None:
            raise RuntimeError(f"Node has no cached data.")
        else:
            return self._input_cache
        
    @property
    def output_cache(self) -> dict:
        if self._output_cache is None:
            raise RuntimeError(f"Node has no cached data.")
        else:
            return self._output_cache

    @property 
    @abstractmethod
    def outputs(self) -> tuple:
        return ("out_1", "out_2", "out_3")

    @abstractmethod
    def _run(self, in_1, in_2, in_3 = None) -> tuple:
        return tuple(None for out in self.outputs)
    
    def run(self, ignore_cache : bool = False, verbose : bool = False, **input_dict) -> NodeRunOutput:
        # check if input chached
        if self.cache_input and not ignore_cache:
            if self._inputEquality(input_dict):
                if self._output_cache is not None:
                    if verbose:
                        print(f"{self.__str__()}: Using cached data.")
                    return self._output_cache
            else:
                self._copyInput(input_dict, verbose)
        # check data in contains all necessary input
        for input_str in self.mandatory_inputs:
            if input_str not in input_dict:
                raise TypeError(f"Node {self} is missing input: '{input_str}'.")
        # check for redudant inputs
        for input_str in input_dict.keys():
            if input_str not in self.inputs:
                raise TypeError(f"Node {self} got an unexpected input '{input_str}'.")
            
        # compute output
        # catch warnings 
        # with warnings.catch_warnings(record=True) as w:
        #     # Cause all warnings to always be triggered.
        #     warnings.simplefilter("always")
        #     # warnings.simplefilter("ignore", category=DeprecationWarning)
        try:
            run_kwds = {}
            if self.has_cache_ignore_option:
                run_kwds["ignore_cache"] = ignore_cache
            if self.has_verbose_option:
                run_kwds["verbose"] = verbose
            output_tuple = self._run(**run_kwds, **input_dict)
        except Exception as e:
            raise RuntimeError(f"Node {self} experienced an errror while excecuting.") from e
            
            # # Print warning
            # if len(w):
            #     print(f"Warnings from node {self}:")
            #     for warn in w:
            #         print(f"\t{warn.message}")

        # check if tuple otherwise create one
        if not isinstance(output_tuple, (tuple,list)):
            output_tuple = (output_tuple,)
        else:
            output_tuple = output_tuple
        # check output contains all expected outputs
        if len(output_tuple) != len(self.outputs):
            raise RuntimeError(f"Node produced {len(output_tuple)} outputs, however expected {len(self.outputs)}.")
        
        # convert to node output
        output = NodeRunOutput(self, self.outputs, output_tuple)
        
        # check if data should be cached
        if self.cache_output and not ignore_cache:
            self._copyOutput(output, verbose)

        # return output
        return output
    
    def _inputEquality(self, dict_ : dict) -> bool:
        if self._input_cache is not None:
            if dict_.keys() != self._input_cache.keys():
                return False
            return all(np.array_equal(dict_[key], self._input_cache[key]) for key in dict_)
        else:
            return False
        
    def _copyInput(self, dict_ : dict, verbose : bool):
        self._input_cache = {}
        for k,v in dict_.items():
            try:
                self._input_cache[k] = copy.deepcopy(v)
            except:
                if verbose:
                    print(f"Could not copy input '{k}' of node {self}, copying only reference.")
                self._input_cache[k] = v

    def _copyOutput(self, output : tuple, verbose : bool):
        output_cache = [None]*len(output)
        for i,v in enumerate(output):
            try:
                output_cache[i] = copy.deepcopy(v)
            except:
                if verbose:
                    print(f"Could not copy output '{self.outputs[i]}' of node {self}, copying only reference.")
                output_cache[i] = v
        self._output_cache = NodeRunOutput(self, self.outputs, output_cache)
        
    def resetCache(self) -> None:
        self._input_cache = None
        self._output_cache = None

    def _createInputOutputAttributes(self) -> None:
        self.input = NodeMapping(self, self.inputs, tuple(NodeInput(self, input) for input in self.inputs), True)
        self.output = NodeMapping(self, self.outputs, tuple(NodeOutput(self, output) for output in self.outputs), False)


    def __str__(self):
        descr = f" - {self.description}" if self.description else "" 
        return type(self).__name__ + descr
    
    def __repr__(self):
        return f"{self.__str__()}\n" \
                    "Inputs: " + str(self.inputs) + "\n" \
                        "Defaults: " + str(self.default_inputs) + "\n" \
                            "Outputs: " + str(self.outputs) + "\n"
    # def __copy__(self):
    #     cls = self.__class__
    #     result = cls.__new__(cls)
    #     result.__dict__.update(self.__dict__)
    #     return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k not in ["_input_cache", "_output_cache"]:
                setattr(result, k, copy.deepcopy(v, memo))
            else:
                 setattr(result, k, None)
        return result

    def copy(self):
        return self.deepcopy()
    
    def deepcopy(self):
        return copy.deepcopy(self)

    def __getstate__(self):
        d = self.__dict__
        d.update({"_input_cache" : None, "_output_cache" : None})
        return d
    def __setstate__(self, d):
        self.__dict__ = d
    
class NodeDict:
    def __init__(self, owner: ProcessNode, keys : Iterable, iterable : Iterable) -> None:
        super().__init__()
        self.__owner = owner
        self.__keys = keys
        self.__tuple = tuple(iterable)

    @property
    def _owner(self) -> ProcessNode:
        return self.__owner
    
    @property
    def keys(self) -> Iterable:
        return self.__keys
    
    @property
    def values(self) -> Iterable:
        return self.__tuple
        
    def __len__(self):
        return len(self.__tuple)
    
    def __getitem__(self, key):
        return self.__getattr__(key)

    def __getattr__(self, key):
        if isinstance(key, int):
            return self.__tuple[key]
        elif isinstance(key, (tuple,list)):
            return tuple(self.__getattr__(k) for k in key)
        elif key in self.__keys:
            return self.__tuple[self.__keys.index(key)]
        elif key == 'all':
            return self.__tuple
        else:
            return object.__getattribute__(self, key)
        
    def __iter__(self):
        for data in self.values:
            yield data
        
    def __str__(self) -> str:
        return f"{self.keys} -> {self.values}"
    
    def __repr__(self) -> str:
        return self.__str__()

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, copy.deepcopy(v, memo))
        return result

    def __getstate__(self):
        return self.__dict__
    def __setstate__(self, d):
        self.__dict__ = d
    
class NodeInputOutput:
    def __init__(self, owner : ProcessNode, name : str) -> None:
        self.__owner = owner
        self.__name = name
    @property
    def owner(self) -> ProcessNode:
        return self.__owner
    @property
    def name(self) -> ProcessNode:
        return self.__name
    @name.setter
    def name(self, val) -> None:
        if self.name == "":
            self.__name = val
        else:
            raise ValueError("Name already set.")
        
    def __hash__(self) -> int:
        return hash((self.owner, self.name))
        
    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, type(self)):
            return self.owner == other.owner and self.name == other.name
        return False
        
    def __str__(self):
        return f"{self.owner}: {self.name}"
    def __repr__(self):
        return self.__str__()
    
    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, copy.deepcopy(v, memo))
        return result
    
    def __getstate__(self):
        return self.__dict__
    def __setstate__(self, d):
        self.__dict__ = d
    
class NodeInput(NodeInputOutput):
    pass

class NodeOutput(NodeInputOutput):
    def __init__(self, owner: ProcessNode, name: str, attributes = tuple()) -> None:
        super().__init__(owner, name)
        self._attributes = attributes
    
    def _getData(self, data):
        for attribute in self._attributes:
            data = attribute(data)
        return data
    
    def __getattr__(self, attribute):
        try:
            return object.__getattribute__(self, attribute)
        except:
            # assume attribute is to be used for transfer
            return NodeOutput(self.owner, self.name, self._attributes + (self.OutputAttribute(attribute), ))
    
    def __call__(self, *args: warnings.Any, **kwds: warnings.Any) -> warnings.Any:
        return NodeOutput(self.owner, self.name, self._attributes + (self.OutputMethod(args, kwds), ))
    
    def __getitem__(self,index):
        return NodeOutput(self.owner, self.name, self._attributes + (self.OutputItem(index), ))
    
    def __eq__(self, other : object):
        return super().__eq__(other) and self._attributes == other._attributes
    
    def __hash__(self) -> int:
        return hash((self.owner, self.name, self._attributes))
    
    def _checkBinaryOperand(self, other):
        if not isinstance(other, NodeOutput):
            raise ValueError("Can only combine a NodeOutput with another NodeOutput.")
    def __add__(self, other : NodeOutput):
        self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__add__").output.output
    def __sub__(self, other : NodeOutput):
        self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__sub__").output.output
    def __mul__(self, other : NodeOutput):
        self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__mul__").output.output
    def __truediv__(self, other : NodeOutput):
        self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__truediv__").output.output
    def __div__(self, other : NodeOutput):
        self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__div__").output.output
    
    class AbstractAttribute(ABC):
        @abstractmethod
        def __call__(self, data):
            return data
        @abstractmethod
        def __hash__(self) -> int:
            return super().__hash__()
        def __eq__(self, __value: object) -> bool:
            return isinstance(__value, type(self))
    class OutputAttribute(AbstractAttribute):
        def __init__(self, attribute) -> None:
            self.attribute = attribute
        def __call__(self, data):
            return data.__getattribute__(self.attribute)
        def __eq__(self, __value: object) -> bool:
            return super().__eq__(__value) and self.attribute == __value.attribute
        def __hash__(self) -> int:
            return hash(self.attribute)
    class OutputMethod(AbstractAttribute):
        def __init__(self, args, kwds) -> None:
            self.args = args
            self.kwds = kwds
        def __call__(self, data):
            return data.__call__(*self.args, **self.kwds)
        def __eq__(self, __value: object) -> bool:
            return super().__eq__(__value) and self.args == __value.args and self.kwds == __value.kwds
        def __hash__(self) -> int:
            return hash((self.args, tuple(sorted(self.kwds.items()))))
    class OutputItem(AbstractAttribute):
        def __init__(self, index) -> None:
            self.index = index
        def __call__(self, data):
            return data.__getitem__(self.index)
        def __eq__(self, __value: object) -> bool:
            return super().__eq__(__value) and self.index == __value.index
        def __hash__(self) -> int:
            try:
                return hash(self.index)
            except:
                return hash(self.index.__reduce__())
        
        
class NodeMapping(NodeDict):
    def __init__(self,  owner: ProcessNode, keys: Iterable, iterable: Iterable, is_input : bool) -> None:
        super().__init__(owner, keys, iterable)
        self.__input_output_str = "input" if is_input else "output"

    @property
    def _input_output_str(self):
        return self.__input_output_str

    def __getattr__(self, key):
        try:
            return super().__getattr__(key)
        except:
            # for some reason parallel processing require direct mapping to properties are required
            raise ValueError(f"{object.__getattribute__(self, '_NodeDict__owner')} does not have an {object.__getattribute__(self, '_NodeMapping__input_output_str')} named '{key}'.")

    def __str__(self) -> str:
        return f"{self._owner}: {self._input_output_str} -> {self.keys}"

class NodeRunOutput(NodeDict):
    def __getattr__(self, key):
        try:
            return super().__getattr__(key)
        except:
            # for some reason parallel processing require direct mapping to properties are required
            raise ValueError(f"{object.__getattribute__(self, '_NodeDict__owner')} does not have an output named '{key}'.")
        
    # def __str__(self) -> str:
    #     return f"{self._owner}: Produced output {super().__str__()}"
    
    def __repr__(self) -> str:
        outputs = []
        for output, value in zip(self.keys, self.values):
            outputs.append(f"{output} -> {value}")
        return f"{self._owner}: Computed output\n" + "\n".join(outputs)

class ProcessWorkflow(ProcessNode):
    """
    The ProcessWorkflow class defines how data flows between different ProcessNodes.
    It is itself a ProcessNode and can thus be part of a larger workflow.
    It takes as its input the nodes in the workflow and a map which describes how data flows between nodes.

    Arguments (in addition to arguments of ProcessNodes):
    ----------
    nodes: 
        List of ProcessNodes, all nodes needs to have unique name, which is different from the name of the workflow itself.
    map: 
        List of strings where each string describes a mapping of the form:

            node_name1 : output_name1 -> node_name2 : input_name1

        where node_name1 and node_name2 are names of nodes provided to the workflow and
        output_name1 and input_name1 are output/input of node1/node2 respectively. 
        It also possible to only specify one of output_name1/input_name1, then they will be assumed to be the same.

        To map a nodes input/output to the workflows input/output, simply use the name of the workflow:
            
            To map a workflow input, write:
                workflow_name : input_name -> node_name1 : input_name

            To map a workflow output, write:
                node_name1 : output_name -> workflow_name : output_name
        
        To map all inputs/outputs to the workflow, simple ommit the input/output_name:
            workflow_name : -> node_name1 :
            node_name1 : -> workflow_name :
        The names of the inputs/outputs will be taken from node_name1, also its default values if there exist any.
    minimal_execution: 
        A boolean that when set to 'True', only those nodes that affect the output of the workflow will be executed (runned).
        Default 'True'.

    Attributes (in addition to attributes of ProcessNodes):
    -----------
    nodes: 
        List of nodes
    node_names: 
        List of node names
    execution_order: 
        Order of execution of nodes
    executing_nodes: 
        Nodes that will executed (runned).
    non_executing_nodes:
        Nodes that will not be executed (runned).
    
    """
    def __init__(self, map : tuple[tuple], description : str = "", minimal_execution = True) -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        # minimize execution by only running nodes necessary to produce outputs of workflow
        self._minimal_execution = bool(minimal_execution)
        # init nodes
        self._initNodes(map)
    
    # mandatory methods
    @property
    def outputs(self) -> tuple:
        return tuple(self._internal_data_map_in[self].keys())
    
    
    def _run(self, ignore_cache : bool, verbose : bool, **input_dict) -> tuple:
        # check nodes intialized
        if not self._nodes_init:
            raise RuntimeError(f"Nodes of workflow has not been initalized.")
        # add non mandatory inputs
        self._addNonMandatoryInputs(input_dict)
        # create input data transfer
        input_data_transfer = {node : {} for node in self._internal_nodes}
        # tranfer this workflow inputs to its internal nodes
        self._transferNodeOutputs(NodeRunOutput(self, tuple(input_dict.keys()), tuple(input_dict.values())), input_data_transfer)
        
        # loop through all execution order
        for order in range(1, max(self.execution_order) + 1):
            self._executeNodes(
                    tuple(node_idx for node_idx, node_order in enumerate(self.execution_order) if node_order == order), 
                        input_data_transfer, ignore_cache, verbose)

        # return workflow inputs from its internal nodes
        return tuple(input_data_transfer[self][output] for output in self.outputs)

    # run helper functions
    def _addNonMandatoryInputs(self, input_dict : dict) -> None:
        for key, value in zip(self.non_mandatory_inputs, self.default_inputs):
            if key not in input_dict:
                input_dict[key] = value

    def _executeNodes(self, node_idxs : tuple[int], input_data_transfer : dict, ignore_cache : bool, verbose : bool):
        for node_idx in node_idxs:
            node = self.nodes[node_idx]
            self._transferNodeOutputs(node.run(ignore_cache=ignore_cache, verbose=verbose, **input_data_transfer[node]), input_data_transfer)

    def _transferNodeOutputs(self, node_output : NodeRunOutput, input_data_transfer : dict) -> None:
        for output_str, transfer_dict in self._internal_data_map_out[node_output._owner].items():
            output = node_output[output_str]
            for output_transfer, inputs in transfer_dict.items():
                out_ = output_transfer._getData(output)
                for input in inputs:
                    input_data_transfer[input.owner][input.name] = out_

    # Update ProcessNode Attributes
    @property
    def inputs(self) -> tuple:
        return self.mandatory_inputs + self.non_mandatory_inputs #tuple(self._internal_data_map_out[f"{self.name}:in"].keys())
    
    @property
    def mandatory_inputs(self) -> tuple:
        return self._mandatory_inputs
    
    @property
    def non_mandatory_inputs(self) -> tuple:
        return self._non_mandatory_inputs

    @property
    def default_inputs(self) -> tuple:
        return self._default_inputs
    
    # Workflow specific attributes
    @property
    def nodes(self) -> tuple[ProcessNode]:
        return self._nodes
    
    @property
    def execution_order(self) -> tuple:
        return self._execution_order

    @property
    def executing_nodes(self) -> tuple[ProcessNode]:
        return tuple(node_idx + 1 for node_idx, node_order in enumerate(self.execution_order) if node_order >= 0)
    
    @property
    def non_executing_nodes(self) -> tuple[ProcessNode]:
        return tuple(node_idx + 1 for node_idx, node_order in enumerate(self.execution_order) if node_order < 0)
    
    @property
    def minimal_execution(self) -> bool:
        return self._minimal_execution
    
    @minimal_execution.setter
    def minimal_execution(self, val : bool) -> None:
        self._minimal_execution = bool(val)
        self._initNodes()

    @property
    def map(self) -> tuple[str]:
        return self._map
    
    @map.setter
    def map(self, val : tuple[str]) -> None:
        self._initNodes(val)
    
    # check node compatibility of input/output mapping
    def _initNodes(self, map_ : tuple[tuple]) -> None:
        # variable signaling nodes has been initiliazed
        self._nodes_init = False
        # add node mapping
        self._addMap(map_)
        # create input output
        self._createInputOutputAttributes()
        
        # check all mandatory inputs are available for internal nodes
        for i, node in enumerate(self.nodes):
            for mandatory_input in node.mandatory_inputs:
                if mandatory_input not in self._internal_data_map_in[node]:
                    raise ValueError(f"Node {i} ({node}) is missing mandatory input '{mandatory_input}'.")
                
        # check all non mandatory inputs are available for internal nodes
        for i, node in enumerate(self.nodes):
            for non_mandatory_input in node.non_mandatory_inputs:
                if non_mandatory_input not in self._internal_data_map_in[node]:
                    print(f"Node {i} ({node}) is missing non-mandatory input '{non_mandatory_input}'.")
        
        # execution order
        self._internal_execution_order = [-1] * len(self._internal_nodes)
        # set this workflow execution order to zero
        self._internal_execution_order[0] = 0
        # find execution order for workflow output
        self._findExecutionOrder(len(self._internal_execution_order) - 1, self._internal_execution_order)
        # check for redudant nodes
        redudant_nodes = [node_idx - 1 for node_idx, node_order in enumerate(self._internal_execution_order) if node_order < 0]
        # give warnings messages if redudant nodes found
        if redudant_nodes:
            if self.minimal_execution:
                # non executing nodes
                print(f"Non-executing nodes found in workflow: node {', node '.join(map(str, redudant_nodes))}.")
            else:
                # redudant nodes
                print(f"Redudant nodes found in workflow: node {', node '.join(map(str, redudant_nodes))}.")
                # determine execution order for redudant nodes
                for redudant_node_idx in redudant_nodes:
                    self._findExecutionOrder(redudant_node_idx + 1, self._internal_execution_order)

        # save nodes execution order
        self._execution_order = tuple(self._internal_execution_order[1:-1])
        # update nodes initialized
        self._nodes_init = True

    # create internal node mapping from list
    def _addMap(self, map_ : tuple[tuple[NodeOutput, NodeInput]]):
        # init input/output variables
        outputs = []
        inputs = []

        for i, entry in enumerate(map_):
            # check entry
            assert isinstance(entry, (tuple,list)), f"Incorrect entry {i}"
            assert len(entry) == 2, f"Incorrect entry {i}"
            # unpack
            output, input = entry
            # check for node = None, change to self
            if isinstance(output, str):
                # split string in output name and attribute parts -> this allows for manipulating input to workflow
                output_parts = re.split('([^a-zA-Z_])', output.replace(" ",""), 1)
                output = eval('NodeOutput(self, output_parts[0])' + ''.join(output_parts[1:]))
            if isinstance(input, str):
                input = NodeInput(self, input.replace(" ",""))
            # check if tuple (an one to many mapping)
            if isinstance(input, (tuple,list)):
                assert isinstance(output, NodeOutput), f"Incorrect output defined in entry #{i}"
                for in_ in input:
                    assert isinstance(in_, NodeInput), f"Incorrect input defined in entry #{i}"
                    if output.name == "":
                        assert output.owner == self, f"Incorrect output defined in entry #{i}"
                        outputs.append(NodeOutput(self, in_.name))
                    else:
                        outputs.append(output)
                    inputs.append(in_)
                continue
            # check if tuple (an 'all' mapping)
            if isinstance(output, (tuple,list)) and input.owner == self and input.name == "":
                for out_ in output:
                    assert isinstance(out_, NodeOutput), f"Incorrect output defined in entry #{i}"
                    inputs.append(NodeInput(self, out_.name))
                    outputs.append(out_)
                continue
            # assert types
            assert isinstance(output, NodeOutput), f"Incorrect output defined in entry #{i}"
            assert isinstance(input, NodeInput), f"Incorrect input defined in entry #{i}"
            # check for empty output or input strings
            if output.owner == self and output.name == "" and input.name != "":
                output.name = input.name
            if input.owner == self and output.name != "" and input.name == "":
                input.name = output.name
            
            # check for empty input output, not allowed unless mapped to workflow (handled above)
            if output.name == "" or input.name == "":
                raise ValueError(f"Incorrect defined entry #{i}.")
            
            # add to outputs inputs
            outputs.append(output)
            inputs.append(input)
        # look for binary operand and add there dependencies
        def addOutputsInputsBinaryOperand(binary_output):
            # check binary_output realy is binary
            if isinstance(binary_output.owner, _BinaryOperand):
                # handle input 1
                outputs.append(binary_output.owner.input_1)
                inputs.append(binary_output.owner.input.input_1)
                addOutputsInputsBinaryOperand(binary_output.owner.input_1)
                # handle input 2
                outputs.append(binary_output.owner.input_2)
                inputs.append(binary_output.owner.input.input_2)
                addOutputsInputsBinaryOperand(binary_output.owner.input_2)
        for output in outputs:
            addOutputsInputsBinaryOperand(output)
        
        # add reference to nodes
        self._nodes = tuple(set(filter(lambda node : node != self, map(lambda input_output : input_output.owner, outputs + inputs))))
        # internal names and nodes
        # first 'self' is input to workflow, last 'self' is output of workflow
        self._internal_nodes = (self, ) + self.nodes + (self, )
        # internal data map
        self._internal_data_map_in = {node : {} for node in self._internal_nodes}
        self._internal_data_map_out = {node : {} for node in self._internal_nodes}
        # create matrix defining dependecies between nodes
        self._internal_dependencies = np.zeros((len(self._internal_nodes),len(self._internal_nodes)), dtype = bool)
        
        # check for duplicate inputs
        to_remove = []
        for i, input in enumerate(inputs):
            try:
                idx = inputs[i+1:].index(input) + i + 1
                output_old = outputs[i]
                output_new = outputs[idx]
                # get index old output
                if output_old.owner == self:
                    output_old_idx = "wf_input"
                else:
                    output_old_idx = self._internal_nodes.index(output_old.owner) - 1
                # get index new output
                if output_new.owner == self:
                    output_new_idx = "wf_input"
                else:
                    output_new_idx = self._internal_nodes.index(output_new.owner) - 1
                # get index input
                if input.owner == self:
                    input_idx = "wf_output"
                else:
                    input_idx = self._internal_nodes.index(input.owner) - 1

                print(f"Input mapping to node {input_idx} ({input.owner}) '{input.name}' "\
                               + f"from node {output_old_idx} ({output_old.owner}) '{output_old.name}' is replaced with "\
                                + f"node {output_new_idx} ({output_new.owner}) '{output_new.name}'.")
                to_remove.append(i)
            except:
                pass

        # create temporary place holder for worfklow in data
        input_output_wf = {"mandatory_inputs" : [], "non_mandatory_inputs" : [], "default_inputs" : [], "direct_mapping" : []}
        # add all entreis
        new_map = []
        for idx, output, input in zip(range(len(outputs)), outputs, inputs):
            if idx not in to_remove:
                self._addEntry(output, input, input_output_wf)
                # check if outout is wf
                # if output.owner != self:
                #     out_ = output
                # else:
                #     out_ = output.name
                # # check if input is wf
                # if input.owner != self:
                #     in_ = input
                # else:
                #     in_ = input.name
                # new_map.append((out_,in_))
                new_map.append((output,input))

        # list with inputs to workflow (direct mapping is mapping directly between input and output of workflow)
        self._mandatory_inputs = tuple(input_output_wf["mandatory_inputs"] + input_output_wf["direct_mapping"])
        self._non_mandatory_inputs = tuple(input_output_wf["non_mandatory_inputs"])
        self._default_inputs = tuple(input_output_wf["default_inputs"])

        # save map
        self._map = tuple(new_map)

    # add an single map between internal nodes
    def _addEntry(self, output : NodeOutput, input : NodeInput, input_output_wf : dict) -> None:
        # unpack tuple
        output_node, output_str, output_trans = output.owner, output.name, output
        input_node, input_str = input.owner, input.name
    
        # check if outputs list already created in internal map
        if output_str not in self._internal_data_map_out[output_node]:
            self._internal_data_map_out[output_node][output_str] = {}
        if output_trans in self._internal_data_map_out[output_node][output_str]:
            self._internal_data_map_out[output_node][output_str][output_trans].append(input)
        # otherwise create new list
        else:
            self._internal_data_map_out[output_node][output_str][output_trans] = [input]

        # add to interal in map
        self._internal_data_map_in[input_node][input_str] = output

        # add dependencies if workflow not involed
        if output_node == self:
            # (input to workflow)
            output_idx = 0
        else:
            output_idx = self._internal_nodes.index(output_node)
        if input_node == self:
            # (output of workflow)
            input_idx = -1
        else:
            input_idx = self._internal_nodes.index(input_node)
        self._internal_dependencies[output_idx,input_idx] = True

        # check if workflow is output node, then update mandatory/non-mandatory inputs
        if output_node == self:
            # check first out node_in is workflow is self
            if input_node == self:
                # check if output not alteady added
                if output_str not in input_output_wf["non_mandatory_inputs"] and \
                    output_str not in input_output_wf["mandatory_inputs"] and \
                        output_str not in input_output_wf["direct_mapping"]:
                    # store as direct mapping between workflow input/output
                    input_output_wf["direct_mapping"].append(output_str)
            # otherwise node_in is internal
            else:
                # check if output already assigned as non mandatory inputs
                if output_str in input_output_wf["non_mandatory_inputs"]:
                    # get index
                    idx = input_output_wf["non_mandatory_inputs"].index(output_str)
                    # check if input is non mandatory in node_in
                    if input_str in input_node.non_mandatory_inputs:
                        # if yes, check if default values are not the same
                        if input_output_wf["default_inputs"][idx] != input_node.default_inputs[input_node.non_mandatory_inputs.index(input_str)]:
                            # if not change input to mandatory
                            del input_output_wf["non_mandatory_inputs"][idx]
                            del input_output_wf["default_inputs"][idx]
                            input_output_wf["mandatory_inputs"].append(output_str)
                    # if input is mandatory in node_in
                    else:
                        # change input to mandatory
                        del input_output_wf["non_mandatory_inputs"][idx]
                        del input_output_wf["default_inputs"][idx]
                        input_output_wf["mandatory_inputs"].append(output_str)
                # if input is already in mandatory inputs, leave it there
                elif output_str in input_output_wf["mandatory_inputs"]:
                    pass
                # else if it has not been added yet
                else:
                    # check if output added to direct mapping
                    if output_str in input_output_wf["direct_mapping"]:
                        # then remove it
                        del input_output_wf["direct_mapping"][input_output_wf["direct_mapping"].index(output_str)]
                    # proceed by adding output to either mandatory or non mandatory inputs
                    # check if input of input node is mandatory
                    if input_str in input_node.mandatory_inputs:
                        # store as mandatory input
                        input_output_wf["mandatory_inputs"].append(output_str)
                    else:
                        # store as non-mandatory input
                        input_output_wf["non_mandatory_inputs"].append(output_str)
                        # store default input
                        input_output_wf["default_inputs"].append(input_node.default_inputs[input_node.non_mandatory_inputs.index(input_str)])

    # determine exectution order of map
    def _findExecutionOrder(self, node_idx : int, execution_order : list[int]) -> int:
        # get execution order
        order = execution_order[node_idx]
        # check if order already resolved
        if order >= 0:
            return order
        # if not, check node's dependencies
        elif order == -1:
            # mark we are checking order for this node by setting order to -2
            execution_order[node_idx] = -2
            # loop through dependencies
            dependencies_execution_order = [0]
            for dependend_node in np.where(self._internal_dependencies[:,node_idx])[0]:
                dependencies_execution_order.append(self._findExecutionOrder(dependend_node, execution_order))
            # execution order for this node is equal to the maximum of its dependecies plus one
            execution_order[node_idx] = max(dependencies_execution_order) + 1
            return execution_order[node_idx]
        # this node has already been looked at and reoccured -> hence cycle dependencies
        if order == -2:
            raise ValueError(f"Cycle dependency found for node {node_idx}")

class ValueNode(ProcessNode):
    """
    Adds a constant value to a workflow
    """
    outputs =  ("value", )

    def __init__(self, value, description : str = "") -> None:
        # call super init
        super().__init__(description)
        self._value = value

    def _run(self):
        return self._value, 
    
    @property
    def value(self):
        return self._value
    
    @value.setter
    def value(self, val):
        self._value = val

class CustomNode(ProcessNode):
    """
    A simple node with just one input and one output.

    Argument:
    ---------
    run_function: 
        the function that produce the output from the input.
    """

    def __init__(self, run_function : Callable[[object], object], outputs : tuple = ("output",), description : str = "") -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        self._run_function = run_function
        # check if string 
        if isinstance(outputs, str):
            self._outputs = (outputs,)
        elif isinstance(outputs, (tuple,list)):
            self._outputs = outputs
        else:
            raise ValueError(f"Argument outputs needs to be a string or tuple, '{type(outputs)}' given.")
        self._createInputOutputAttributes()

    @property
    def outputs(self) -> tuple:
        return self._outputs
    def _run(self, **kw_args):
        return self._run_function(**kw_args) 

    # redefine properties
    @property
    def inputs(self) -> tuple:
        return tuple(inspect.getfullargspec(self._run_function).args)
    
    @property
    def default_inputs(self) -> tuple:
        if self._run_function.__defaults__ is None:
            def_args = tuple()
        else:
            def_args = self._run_function.__defaults__
        return def_args
    

def _iteratingFunction(run_function : Callable[[object], object], obj):
    return run_function(obj)

def _runIterNode(node : ProcessNode, common_input_dict : dict, arg_names : list[str], arg_values : tuple):
    return node.run(**common_input_dict, **{name : arg for name, arg in zip(arg_names, arg_values)})

def is_numeric(obj) -> bool:
    attrs = ['__add__', '__sub__', '__mul__', '__truediv__', '__pow__']
    return all(hasattr(obj, attr) for attr in attrs)

class IteratingNode(ProcessNode):
    """
    An iterating node which iterates through a list of arguements and pass them to the internal iterating node and stores the outputs in lists.

    Argument:
    ---------
    iterating_node: 
        the node to be iterated
    iterating_inputs: 
        the inputs of the iterating_node to be iterated over
    parallel_processing: 
        whether not to execute the iteration in parallel processes
    nr_processes: 
        number of process if parallel_processing is True, negative number signals to use the maxmimum number of available cores
    
    Attributes:
        inputs: 
            the inputs to which to be iterated over has '_list' appended to their name
        outputs:
            the outputs from the iterating node are appended to an iteration list, hence they have '_list' appended to their name
    """
    def __init__(self, iterating_node : ProcessNode, iterating_inputs : tuple[str], parallel_processing : bool = False, nr_processes : int = -1, description : str = "") -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        # save arguments
        self._iterating_node = iterating_node
        self.parallel_processing = parallel_processing
        self.nr_processes = nr_processes
        
        # make sure iterating inputs is a list
        if isinstance(iterating_inputs, str):
            iterating_inputs = (iterating_inputs, )
        # create variables
        mandatory_inputs = list(iterating_node.mandatory_inputs)
        non_mandatory_inputs = list(iterating_node.non_mandatory_inputs)
        default_inputs = list(iterating_node.default_inputs)
        # check input exist and _list to its name
        for input in iterating_inputs:
            if input in mandatory_inputs:
                mandatory_inputs[mandatory_inputs.index(input)] = self._listName(input)
            elif input in non_mandatory_inputs:
                default_inputs[non_mandatory_inputs.index(input)] = [default_inputs[non_mandatory_inputs.index(input)]]
                non_mandatory_inputs[non_mandatory_inputs.index(input)] = self._listName(input)
            else:
                raise ValueError(f"Input {input} does not exist in node: {iterating_node}.")
        # create tuples
        self._iterating_inputs = tuple(iterating_inputs)
        self._mandatory_inputs = tuple(mandatory_inputs)
        self._non_mandatory_inputs = tuple(non_mandatory_inputs)
        self._default_inputs = tuple(default_inputs)
        # create outputs tuple and ad _list to all names
        self._outputs = tuple(self._listName(output) for output in iterating_node.outputs)
        # create attributes
        self._createInputOutputAttributes()

    @property
    def outputs(self) -> tuple:
        return self._outputs

    def _run(self, verbose : bool, **input_dict):
        # check input_dict
        nr_iter = self._checkInput(input_dict)
        # iterating inputs
        arg_values_list = [ input_dict[self._listName(iter_input)] for iter_input in self.iterating_inputs ] 
        # create internal node input
        common_input_dict = input_dict.copy()
        # remove list names
        for iter_input in self.iterating_inputs:
            common_input_dict.pop(self._listName(iter_input))
        # create call functiona
        # f_multi_iteration = lambda arg_values : self._iterNodeMap(self.iterating_node, common_input_dict, self.iterating_inputs, arg_values)
        
        
        # Check if parallel processing or not
        if self.parallel_processing and nr_iter > 1:
            with warnings.catch_warnings() as w:
            # Cause all warnings to always be triggered.
                warnings.simplefilter("error", category=DeprecationWarning)
                # queue to update tqdm process bar
                pbar_queue = Queue()
                # process to update tqdm process bar
                pbar_proc = Process(target=self._pbarListener, args=(pbar_queue, nr_iter, f"{self} (parallel - {self.nr_processes})", verbose))
                # process to execute
                processes = [self._createProcessAndPipe(self._iterNode, self.iterating_node, pbar_queue, verbose, common_input_dict, self.iterating_inputs, arg_values) for arg_values in self._iterArgs(nr_iter, self.nr_processes, arg_values_list)]
                # start processes
                pbar_proc.start()
                [p[1].start() for p in processes]
            # get result
            process_results = [p[0].recv() for p in processes]
            # wait for them to finnish
            [p[1].join() for p in processes]
            [p[0].close() for p in processes]
            # terminate pbar_process by sending None to queue
            pbar_queue.put(None)
            # close queue and wait for backround thread to join
            pbar_queue.close()
            pbar_queue.join_thread()
            # join pbar process
            pbar_proc.join()
            # combine process_results
            # mapped = chain.from_iterable(process_results)
            return tuple(np.concatenate(output_list) if isinstance(output_list[0], np.ndarray) else tuple(chain.from_iterable(output_list)) for output_list in zip( *process_results ))
        else:
            f_single_iteration = lambda args : self.iterating_node.run(ignore_cache=True, verbose=verbose, **common_input_dict, **{name : arg for name, arg in zip(self.iterating_inputs, args)}).values
            if verbose:
                mapped = map(f_single_iteration, tqdm(zip(*arg_values_list), total = nr_iter, desc = f"{self} (sequential)"))
            else:
                mapped = map(f_single_iteration, zip(*arg_values_list))
            return  tuple(np.array(output) if is_numeric(output[0]) else output for output in zip( *mapped ))

        # return tuple( zip( *mapped ) )
        # return  tuple(np.array(output) if is_numeric(output[0]) else output for output in zip( *mapped ))
    
    @staticmethod
    def _pbarListener(pbar_queue, nr_iter, desc, show_progress):
        if show_progress:
            pbar = tqdm(total = nr_iter, desc = desc)
            for nr in iter(pbar_queue.get, None):
                pbar.update(nr)
    
    @staticmethod
    def _createProcessAndPipe(target, *args):
        in_pipe, out_pipe = Pipe(duplex = False)
        p = Process(target = target, args = args, kwargs = {"pipe" : out_pipe})
        return in_pipe, p

    @staticmethod
    def _iterNode(iterating_node : ProcessNode, pbar_queue : Queue, verbose : bool, common_input_dict : dict, arg_names : list[str], arg_values : Iterable, pipe : Pipe) -> list:
        outputs = []
        nr_iter, iterable_list = arg_values
        update_every = int(nr_iter / 100)
        nr = 0
        for args in zip(*iterable_list):
            outputs.append(iterating_node.run(ignore_cache=True, verbose=verbose, **common_input_dict, **{name : arg for name, arg in zip(arg_names, args)}).values)
            nr += 1
            if nr > update_every:
                pbar_queue.put(nr)
                nr = 0
        pbar_queue.put(nr)
        pipe.send(tuple(np.array(output) if is_numeric(output[0]) else output for output in zip( *outputs )))
        # pipe.send(outputs)
        pipe.close()

    @staticmethod
    def _iterArgs(nr_iter, nr_chunks, iterable_list):
        chunk_size, extra = divmod(nr_iter, nr_chunks)
        if extra:
            chunk_size += 1
        idx = [i * chunk_size for i in range(nr_chunks) if i * chunk_size < nr_iter]
        idx.append(nr_iter)

        for i in range(len(idx) - 1):
            yield (idx[i + 1] - idx[i], tuple(l[idx[i] : idx[i + 1]] for l in iterable_list) )

    def _checkInput(self, input_dict : dict) -> int:
        lenghts = set()
        for iter_input in self.iterating_inputs:
            # check if iter_input given
            if self._listName(iter_input) in input_dict:
                lenghts.add(len(input_dict[self._listName(iter_input)]))
            # if not it must be a non mandatory input (or error would already been raised by parrent clase)
            else:
                input_dict[self._listName(iter_input)] = self.default_inputs[self.non_mandatory_inputs.index(self._listName(iter_input))]
                lenghts.add(1)
        # check lenghts are consisted
        if len(lenghts) == 0:
            lenghts.add(0)
        # length of 1 is length of default value, must be handle with some care
        elif len(lenghts) > 1 and 1 in lenghts:
            lenghts.remove(1)
        # check lenghts
        if len(lenghts) != 1:
            raise ValueError("Inconsistent lengths given for the iterating inputs.")
        else:
            nr_iter = lenghts.pop()
        # make sure all iterating inputs with length one
        for iter_input in self.iterating_inputs:
            val = input_dict[self._listName(iter_input)]
            len_ = len(val)
            if len_ != nr_iter:
                if len_ == 1:
                    input_dict[self._listName(iter_input)] = val * nr_iter
                else:
                    raise ValueError("Inconsistent lengths given for the iterating inputs, coding error...")
        return nr_iter

    def _listName(self, name : str) -> str:
        return f"{name}_list"

    # new properties
    @property
    def iterating_node(self) -> ProcessNode:
        return self._iterating_node
    
    @property
    def iterating_inputs(self) -> tuple:
        return self._iterating_inputs
    
    @property
    def parallel_processing(self) -> bool:
        return self._parallel_processing

    @parallel_processing.setter
    def parallel_processing(self, val) -> None:
        self._parallel_processing = bool(val)
    
    @property
    def nr_processes(self) -> int:
        return self._nr_processes
    
    @nr_processes.setter
    def nr_processes(self, val : int) -> None:
        if val <= -1:
            # raise ValueError("Number of threads needs to be larger than 0.")
            self._nr_processes = cpu_count()
        else:
            self._nr_processes = int(val)

    # modified properties
    @property
    def inputs(self) -> tuple:
        return self.mandatory_inputs + self.non_mandatory_inputs
    
    @property
    def mandatory_inputs(self) -> tuple:
        return self._mandatory_inputs
    
    @property
    def non_mandatory_inputs(self) -> tuple:
        return self._non_mandatory_inputs
    
    @property
    def default_inputs(self) -> tuple:
        return self._default_inputs
    
class _BinaryOperand(ProcessNode):
    outputs = ("output",)
    def __init__(self, input_1, input_2, operand : str, description: str = "", create_input_output_attr: bool = True) -> None:
        super().__init__(description, create_input_output_attr)
        self.operand = operand
        self.input_1 = input_1
        self.input_2 = input_2

    def _run(self, input_1, input_2) -> tuple:
        return getattr(input_1, self.operand)(input_2)
    
class EntryNode(ProcessNode):
    def __init__(self, entry : str, description: str = "", create_input_output_attr: bool = True) -> None:
        self._outputs = (entry, )
        super().__init__(description, create_input_output_attr)

    def _run(self, **kwds) -> tuple:
        return kwds[self._outputs[0]]
        
    # modified properties
    @property
    def outputs(self) -> tuple:
        return self._outputs
    
    @property
    def inputs(self) -> tuple:
        return self._outputs
    
    @property
    def mandatory_inputs(self) -> tuple:
        return self.inputs
    
    @property
    def non_mandatory_inputs(self) -> tuple:
        return tuple()
    
    @property
    def default_inputs(self) -> tuple:
        return tuple()