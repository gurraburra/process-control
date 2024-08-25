from __future__ import annotations
from abc import ABC, abstractmethod
import warnings
import inspect
import numpy as np
import copy
from ._NodeMappings import NodeRunOutput, NodeRunInput, NodeMapping, NodeInputOutput
import operator

class ProcessNode(object):
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
    def has_ignore_cache_option(self):
        return "ignore_cache" in inspect.getfullargspec(self._run).args

    @property
    def has_update_cache_option(self):
        return "update_cache" in inspect.getfullargspec(self._run).args
    
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
        
    def resetCache(self) -> None:
        self._input_cache = None
        self._output_cache = None

    @property 
    @abstractmethod
    def outputs(self) -> tuple:
        return ("out_1", "out_2", "out_3")

    @abstractmethod
    def _run(self, in_1, in_2, in_3 = None) -> tuple:
        return tuple(None for out in self.outputs)
    
    def run(self, ignore_cache : bool = False, update_cache : bool = False, verbose : bool = False, **input_dict) -> NodeRunOutput:
        # check inputs
        self._checkRunInputs(input_dict)
        # add non mandatory inputs
        self._addNonMandatoryInputs(input_dict)

        # check if input chached
        if not ignore_cache and self.cache_input:
            if not update_cache and self._inputEquality(input_dict):
                if self._output_cache is not None:
                    if verbose:
                        print(f"{self.__str__()}: Using cached data.")
                    return self._output_cache
            else:
                # self._copyInput(input_dict)
                self._input_cache = NodeRunInput(self, list(input_dict.keys()), list(input_dict.values()))
            
        # compute output
        # catch warnings 
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            # warnings.simplefilter("always")
            # warnings.simplefilter("ignore", category=DeprecationWarning)
            try:
                run_kwds = {}
                if self.has_ignore_cache_option:
                    run_kwds["ignore_cache"] = ignore_cache
                if self.has_update_cache_option:
                    run_kwds["update_cache"] = update_cache
                if self.has_verbose_option:
                    run_kwds["verbose"] = verbose
                output_tuple = self._run(**run_kwds, **input_dict)
            except Exception as e:
                raise RuntimeError(f"Node {self} experienced an errror while excecuting.") from e
            
            # Print warning
            if len(w):
                print(f"Warnings from node {self}:")
                for warn in w:
                    print(f"\t{warn.message}")

        # check if tuple otherwise create one
        if not isinstance(output_tuple, (tuple,list)):
            output_tuple = (output_tuple,)
        else:
            output_tuple = output_tuple
        # check output contains all expected outputs
        if len(output_tuple) != len(self.outputs):
            raise RuntimeError(f"Node {self} produced {len(output_tuple)} outputs, however expected {len(self.outputs)}.")
        
        # convert to node output
        output = NodeRunOutput(self, self.outputs, output_tuple)
        
        # check if data should be cached
        if not ignore_cache and self.cache_output:
            # self._copyOutput(output)
            self._output_cache = output

        # return output
        return output
    
    # run helper functions
    def _checkRunInputs(self, input_dict : dict) -> None:
        # check data in contains all necessary input
        for input_str in self.mandatory_inputs:
            if input_str not in input_dict:
                raise TypeError(f"Node {self} is missing input: '{input_str}'.")
        # check for redudant inputs
        for input_str in input_dict.keys():
            if input_str not in self.inputs:
                raise TypeError(f"Node {self} got an unexpected input '{input_str}'.")

    def _addNonMandatoryInputs(self, input_dict : dict) -> None:
        for key, value in zip(self.non_mandatory_inputs, self.default_inputs):
            if key not in input_dict:
                input_dict[key] = value
    
    def _inputEquality(self, dict_ : dict) -> bool:
        if self._input_cache is not None:
            if dict_.keys() != set(self._input_cache.keys()):
                return False
            for key in dict_:
                # allow nan values
                try:
                    b = np.array_equal(dict_[key], self._input_cache[key], equal_nan=True)
                # wont work for objects, ignore nans
                except TypeError:
                    b = np.array_equal(dict_[key], self._input_cache[key], equal_nan=False)
                if not b:
                    return False
            return True
        else:
            return False
        
    def _copyInput(self, dict_ : dict):
        self._input_cache = {}
        for k,v in dict_.items():
            self._input_cache[k] = v
            # try:
            #     self._input_cache[k] = copy.deepcopy(v)
            # except:
            #     if verbose:
            #         print(f"Could not copy input '{k}' of node {self}, copying only reference.")
            #     self._input_cache[k] = v

    def _copyOutput(self, output : tuple):
        # output_cache = [None]*len(output)
        # for i,v in enumerate(output):
        #     output_cache[i] = v
            # try:
            #     output_cache[i] = copy.deepcopy(v)
            # except:
            #     if verbose:
            #         print(f"Could not copy output '{self.outputs[i]}' of node {self}, copying only reference.")
            #     output_cache[i] = v
        self._output_cache = NodeRunOutput(self, self.outputs, output.copy())

    def _createInputOutputAttributes(self) -> None:
        self.input = NodeMapping(self, self.inputs, tuple(NodeInput(self, input) for input in self.inputs), True)
        self.output = NodeMapping(self, self.outputs, tuple(NodeOutput(self, output) for output in self.outputs), False)


    def __str__(self):
        descr = f" - '{self.description}'" if self.description else "" 
        return type(self).__name__ + descr
    
    def __repr__(self):
        return f"{self.__str__()}\n" \
                    "Inputs: " + str(self.mandatory_inputs + tuple(f"{inp}={def_}" for inp,def_ in zip(self.non_mandatory_inputs, self.default_inputs))) + "\n" \
                        "Outputs: " + str(self.outputs)
                        # "Defaults: " + str(self.default_inputs) + "\n" 
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
        # assume attribute is to be used for transfer
        if isinstance(attribute, str) and attribute.isidentifier():
            return NodeOutput(self.owner, self.name, self._attributes + (self.OutputAttribute(attribute), ))
        else:
            raise ValueError(f"Incorrect node output identifier: {attribute}.")
    
    def __call__(self, *args: warnings.Any, **kwds: warnings.Any) -> warnings.Any:
        return NodeOutput(self.owner, self.name, self._attributes + (self.OutputMethod(args, kwds), ))
    
    def __getitem__(self,index):
        return NodeOutput(self.owner, self.name, self._attributes + (self.OutputItem(index), ))
    
    def __eq__(self, other : object):
        return super().__eq__(other) and self._attributes == other._attributes
    
    def __hash__(self) -> int:
        return hash((self.owner, self.name, self._attributes))
    
    def copy(self) -> NodeOutput:
        assert self.owner is None, f"Only allowed to copy unnassigned outputs."
        return NodeOutput(self.owner, self.name, self._attributes)
    
    def _checkBinaryOperand(self, other):
        if not isinstance(other, NodeOutput):
            from ._SimpleNodes import ValueNode
            other = ValueNode(other).output.value
            # raise ValueError("Can only combine a NodeOutput with another NodeOutput.")
        # check if blank output given, marked that name set to None
        if self.name is None or other.name is None:
            raise ValueError("Blank outputs are not valid in binary operations.")
        return other
    def __add__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__add__").output.output
    def __sub__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__sub__").output.output
    def __mul__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__mul__").output.output
    def __truediv__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__truediv__").output.output
    def __floordiv__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__floordiv__").output.output
    def __div__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__div__").output.output
    def __lt__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__lt__").output.output
    def __le__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__le__").output.output
    def __gt__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__gt__").output.output
    def __ge__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__ge__").output.output
    def __eq__(self, other : object):
        other = self._checkBinaryOperand(other)
        return _BinaryOperand(self, other, "__eq__").output.output
    
    # implent iter to tell python this class in not iterable
    def __iter__(self):
        raise TypeError(f"'{type(self).__name__}' object is not iterable")
    
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
            
class _BinaryOperand(ProcessNode):
    outputs = ("output",)
    def __init__(self, input_1, input_2, operand : str, description: str = "", create_input_output_attr: bool = True) -> None:
        super().__init__(description, create_input_output_attr)
        self.operand = operand
        self.input_1 = input_1
        self.input_2 = input_2

    def _run(self, input_1, input_2) -> tuple:
        return getattr(operator, self.operand)(input_1, input_2)