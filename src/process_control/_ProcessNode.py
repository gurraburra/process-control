from __future__ import annotations
from abc import ABC, abstractmethod
import warnings
import inspect
import numpy as np
import copy
from ._NodeMappings import NodeRunOutput, NodeRunInput, NodeMapping, NodeInputOutput
import operator
from functools import partial

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
    # no default condition
    class _UniqueVal:
        pass
    no_default_input = _UniqueVal()

    def __init__(self, description : str = "", create_input_output_attr : bool = True) -> None:
        self._description = description
        self._cache_input = False
        self._cache_output = False
        self._input_cache = None
        self._output_cache = None
        # object set inputs
        self.__object_specific_default_inputs = False
        self.__object_mandatory_inputs = None
        self.__object_non_mandatory_inputs = None
        self.__object_default_inputs = None
        if create_input_output_attr:
            self._createInputOutputAttributes()

    # by class specifiec mandatory/non_mandatory/default inputs
    @property
    def class_inputs(self) -> tuple:
        return tuple(inspect.getfullargspec(self._run).args[1:])
    
    @property
    def class_mandatory_inputs(self) -> tuple:
        return self.class_inputs[:len(self.class_inputs)-len(self.class_default_inputs)]
    
    @property
    def class_non_mandatory_inputs(self) -> tuple:
        return self.class_inputs[len(self.class_inputs)-len(self.class_default_inputs):]
    
    @property
    def class_default_inputs(self) -> tuple:
        if self._run.__defaults__ is None:
            def_args = tuple()
        else:
            def_args = self._run.__defaults__
        return def_args
    
    # possibly object-specific mandatory/non_mandatory/default inputs
    @property
    def object_specific_default_inputs(self):
        return self.__object_specific_default_inputs
        
    @property
    def inputs(self) -> tuple:
        if self.object_specific_default_inputs:
            return self.mandatory_inputs + self.non_mandatory_inputs
        else:
            return self.class_inputs
    
    @property
    def mandatory_inputs(self) -> tuple:
        if self.object_specific_default_inputs:
            return self.__object_mandatory_inputs
        else:
            return self.class_mandatory_inputs
    
    @property
    def non_mandatory_inputs(self) -> tuple:
        if self.object_specific_default_inputs:
            return self.__object_non_mandatory_inputs
        else:
            return self.class_non_mandatory_inputs
    
    @property
    def default_inputs(self) -> tuple:
        if self.object_specific_default_inputs:
            return self.__object_default_inputs
        else:
            return self.class_default_inputs
        
    # other properties
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
    
    # instance methods
    def resetCache(self) -> None:
        self._input_cache = None
        self._output_cache = None
    
    # change which values to use: class or object based
    def __setObjectSpecficDefaultInputs(self, set : bool):
        # store bool
        self.__object_specific_default_inputs = bool(set)
        # check
        if self.__object_specific_default_inputs:
            # store class values if None
            if self.__object_default_inputs is None:
                self.__object_default_inputs = self.class_default_inputs
            if self.__object_non_mandatory_inputs is None:
                self.__object_non_mandatory_inputs = self.class_non_mandatory_inputs
            if self.__object_mandatory_inputs is None:
                self.__object_mandatory_inputs = self.class_mandatory_inputs
        else:
            # rest to None
            self.__object_default_inputs = None
            self.__object_non_mandatory_inputs = None
            self.__object_mandatory_inputs = None

    # change default inputs
    def setDefaultInputs(self, __reset__ = False, **default_inputs):
        # check reset
        if __reset__:
            # change back to use class specific values
            self.__setObjectSpecficDefaultInputs(False)
        # if default inputs given
        if default_inputs:
            # tell object to use object specific values values
            self.__setObjectSpecficDefaultInputs(True)
            # create editable list
            obj_mandatory_inputs = list(self.mandatory_inputs)
            obj_non_mandatory_inputs = list(self.non_mandatory_inputs)
            obj_default_inputs = list(self.default_inputs)
            # loop through inputs
            for input, default_val in default_inputs.items():
                # check if mandatory
                if input in obj_mandatory_inputs:
                    # move to non mandatory if default val is not self.no_default_input
                    if default_val is not self.no_default_input:
                        # remove mandatory
                        obj_mandatory_inputs.remove(input)
                        # add non mandatory
                        obj_non_mandatory_inputs.append(input)
                        obj_default_inputs.append(default_val)
                elif input in obj_non_mandatory_inputs:
                    # if input already is a non mandatory input, update default, if default val is not self.no_default_input
                    if default_val is not self.no_default_input:
                        obj_default_inputs[obj_non_mandatory_inputs.index(input)] = default_val
                    # else move to mandotory inputs
                    else:
                        # add to mandatory
                        obj_mandatory_inputs.append(input)
                        # remove from non mandatory (order is important)
                        del obj_default_inputs[obj_non_mandatory_inputs.index(input)]
                        obj_non_mandatory_inputs.remove(input)
                else:
                    raise ValueError(f"Node {self} has no input '{input}'")
            self.__object_default_inputs = tuple(obj_default_inputs)
            self.__object_non_mandatory_inputs = tuple(obj_non_mandatory_inputs)
            self.__object_mandatory_inputs = tuple(obj_mandatory_inputs)
        # return reference to self
        return self



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
        # # add non mandatory inputs
        # self._addNonMandatoryInputs(input_dict)

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
                # reset output_cache before running
                self._output_cache = None
            
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
    
    def _bestMatch(self, string, values):
        potential_match = []
        len_str = len(string)
        # loop through values
        for val in values:
            len_val = len(val)
            # check length
            if  len_val > len_str:
                continue
            else:
                # check match
                if val == string[:len_val]:
                    potential_match.append(val)
        # check matches
        if len(potential_match) == 1:
            return potential_match[0]
        else:
            return None
    
    # run helper functions
    def _checkRunInputs(self, input_dict : dict) -> None:
        # check inputs 
        for input_str in self.inputs:
            if input_str not in input_dict:
                best_match = self._bestMatch(input_str, input_dict.keys())
                if best_match is not None and best_match not in self.inputs: # make sure best match is not another input
                    input_dict[input_str] = input_dict[best_match]
                    del input_dict[best_match]
                else:
                    if input_str in self.non_mandatory_inputs:
                        input_dict[input_str] = self.default_inputs[self.non_mandatory_inputs.index(input_str)]
                    else:
                        raise TypeError(f"Node {self} is missing input: '{input_str}'.")
        # check for redudant inputs
        for input_str in input_dict.keys():
            if input_str not in self.inputs:
                raise TypeError(f"Node {self} got an unexpected input '{input_str}'.")

    # def _addNonMandatoryInputs(self, input_dict : dict) -> None:
    #     for key, value in zip(self.non_mandatory_inputs, self.default_inputs):
    #         if key not in input_dict:
    #             input_dict[key] = value
    
    def _inputEquality(self, dict_ : dict) -> bool:
        if self._input_cache is not None:
            if dict_.keys() != set(self._input_cache._keys()):
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

    def _createInputOutputAttributes(self, **default_inputs) -> None:
        self.input = NodeMapping(self, self.inputs, tuple(NodeInput(self, input) for input in self.inputs), True)
        self.output = NodeMapping(self, self.outputs, tuple(NodeOutput(self, output) for output in self.outputs), False)
        self.setDefaultInputs(**default_inputs)


    def __str__(self):
        descr = f" - '{self.description}'" if self.description else "" 
        return type(self).__name__ + descr
    
    def __repr__(self):
        sorted_man_inp = tuple(sorted(self.mandatory_inputs))
        sorted_non_mand_idx = sorted(range(len(self.non_mandatory_inputs)), key=lambda k: self.non_mandatory_inputs[k])
        sorted_outputs = tuple(sorted(self.outputs))
        return f"{self.__str__()}\n" \
                    "Inputs: " + ", ".join(sorted_man_inp + tuple(f"{self.non_mandatory_inputs[k]}={f"'{self.default_inputs[k]}'" if isinstance(self.default_inputs[k], str) else str(self.default_inputs[k])}" for k in sorted_non_mand_idx)) + "\n" \
                        "Outputs: " + ", ".join(sorted_outputs)
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

    def copy(self, description = None):
        return self.deepcopy(description)
    
    def deepcopy(self, description = None):
        new_obj = copy.deepcopy(self)
        if description is not None:
            new_obj.description = description
        return new_obj

    def __getstate__(self):
        d = self.__dict__
        d.update({"_input_cache" : None, "_output_cache" : None})
        return d
    def __setstate__(self, d):
        self.__dict__ = d

class NodeInput(NodeInputOutput):
    pass

# binary and unitary operators
def unitary_binary_operators(cls):
    for binop in ['__add__','__sub__','__mul__','__truediv__','__floordiv__','__div__','__lt__','__le__','__gt__','__ge__','__eq__', '__ne__','__and__','__or__','__xor__','__mod__','__pow__','__rshift__','__lshift__']:
        setattr(cls, binop, cls._make_binop(binop))
    for uniop in ['__invert__', '__neg__', '__pos__']:
        setattr(cls, uniop, cls._make_uniop(uniop))
    return cls

# custom unitary and binary operators
class MulInfix(object):
    def __init__(self, func):
        self.func = func
    def __mul__(self, other):
        return self.func(other)
    def __rmul__(self, other):
        return MulInfix(partial(self.func, other))
    def __call__(self, v1, v2):
        return self.func(v1, v2)
    
class MulPrefix(object):
    def __init__(self, func):
        self.func = func
    def __mul__(self, other):
        return self.func(other)
    def __call__(self, v1, v2):
        return self.func(v1, v2)
    
@MulInfix
def _and_(x, y):
    assert isinstance(x, NodeOutput)
    if not x._checkBinaryOperand(y):
        return NotImplemented
    def func(v1, v2):
        return v1 and v2
    return _BinaryOperand(x, y, func).output.output

@MulInfix
def _or_(x, y):
    assert isinstance(x, NodeOutput)
    if not x._checkBinaryOperand(y):
        return NotImplemented
    def func(v1, v2):
        return v1 or v2
    return _BinaryOperand(x, y, func).output.output

@MulInfix
def _xor_(x, y):
    assert isinstance(x, NodeOutput)
    if not x._checkBinaryOperand(y):
        return NotImplemented
    def func(v1, v2):
        return bool(v1) != bool(v2)
    return _BinaryOperand(x, y, func).output.output

@MulPrefix
def not_(x):
    assert isinstance(x, NodeOutput)
    if not x._checkUnitaryOperand():
        return NotImplemented
    def func(v1):
        return not v1
    return _UnitaryOperand(x, func).output.output

@unitary_binary_operators
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
    
    def __getitem__(self, index):
        return NodeOutput(self.owner, self.name, self._attributes + (self.OutputItem(index), ))
    
    def __hash__(self) -> int:
        return hash((self.owner, self.name, self._attributes))
    
    # implent iter to tell python this class in not iterable
    def __iter__(self):
        raise TypeError(f"'{type(self).__name__}' object is not iterable")
    
    def copy(self) -> NodeOutput:
        assert self.owner is None, f"Only allowed to copy unnassigned outputs."
        return NodeOutput(self.owner, self.name, self._attributes)
    
    def _checkBinaryOperand(self, other):
        if not isinstance(other, NodeOutput):
            return False
            # raise ValueError("Can only combine a NodeOutput with another NodeOutput.")
        # check if blank output given, marked that name set to None
        elif self.name is None or other.name is None:
            raise ValueError("Blank outputs are not valid in binary operations.")
        else:
            return True
    
    def _checkUnitaryOperand(self):
        # check if blank output given, marked that name set to None
        if self.name is None:
            raise ValueError("Blank outputs are not valid in unitary operations.")
        else:
            return True
        
    @classmethod
    def _make_binop(cls, operator):
        def binop(self : NodeOutput, other):
            if not self._checkBinaryOperand(other):
                return NotImplemented
            return _BinaryOperand(self, other, operator).output.output
        return binop
    
    @classmethod
    def _make_uniop(cls, operator):
        def uniop(self : NodeOutput):
            self._checkUnitaryOperand()
            return _UnitaryOperand(self, operator).output.output
        return uniop
    
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
    def __init__(self, input_1, input_2, operand, description: str = "", create_input_output_attr: bool = True) -> None:
        super().__init__(description, create_input_output_attr)
        if inspect.isfunction(operand):
            self.operand = operand
        elif isinstance(operand, str):
            self.operand = getattr(operator, operand)
        else:
            raise ValueError(f"invalid operand: {operand}")
        self.input_1 = input_1
        self.input_2 = input_2

    def _run(self, input_1, input_2) -> tuple:
        return self.operand(input_1, input_2)
    
class _UnitaryOperand(ProcessNode):
    outputs = ("output",)
    def __init__(self, input_1, operand, description: str = "", create_input_output_attr: bool = True) -> None:
        super().__init__(description, create_input_output_attr)
        if inspect.isfunction(operand):
            self.operand = operand
        elif isinstance(operand, str):
            self.operand = getattr(operator, operand)
        else:
            raise ValueError(f"invalid operand: {operand}")
        self.input_1 = input_1

    def _run(self, input_1) -> tuple:
        return self.operand(input_1)