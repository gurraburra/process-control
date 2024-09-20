from collections.abc import Callable
import inspect
from ._ProcessNode import ProcessNode

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

    # redfined properties
    # value node should never cache data
    @property
    def cache_data(self) -> bool:
        return False
    
    @cache_data.setter
    def cache_data(self, val : bool) -> None:
        pass

    @property
    def cache_input(self) -> bool:
        return False
    
    @cache_input.setter
    def cache_input(self, val : bool) -> None:
        pass

    @property
    def cache_output(self) -> bool:
        return False
    
    @cache_output.setter
    def cache_output(self, val : bool) -> None:
        pass

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
    def class_inputs(self) -> tuple:
        return tuple(inspect.getfullargspec(self._run_function).args)
    
    @property
    def class_default_inputs(self) -> tuple:
        if self._run_function.__defaults__ is None:
            def_args = tuple()
        else:
            def_args = self._run_function.__defaults__
        return def_args
    
# class EntryNode(ProcessNode):
#     def __init__(self, entry : str, description: str = "", create_input_output_attr: bool = True) -> None:
#         self._outputs = (entry, )
#         super().__init__(description, create_input_output_attr)

#     def _run(self, **kwds) -> tuple:
#         return kwds[self._outputs[0]]
        
#     # modified properties
#     @property
#     def outputs(self) -> tuple:
#         return self._outputs
    
#     @property
#     def class_inputs(self) -> tuple:
#         return self._outputs
    
#     @property
#     def class_mandatory_inputs(self) -> tuple:
#         return self.inputs
    
#     @property
#     def class_non_mandatory_inputsinputs(self) -> tuple:
#         return tuple()
    
#     @property
#     def class_default_inputs(self) -> tuple:
#         return tuple()