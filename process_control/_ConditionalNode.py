from ._ProcessNode import ProcessNode, NodeInput, NodeOutput

class ConditionalNode(ProcessNode):
    """
    A conditioanl node will only run internal node if condition is True.

    Argument:
    ---------
    conditional_node: 
        the node to be conditional run
    conditional_input: 
        the name of the input to decide if of the conditional_node should be run
    default_condition: 
        default condition if the conditional input should not be mandatory
    input_output_map: 
        if conditional_node should not be runned, an addtional map can be given to map input to output
        additionaly, any object can be mapped to an output

    Attributes:
        inputs: 
            the inputs to which to be iterated over has '_list' appended to their name
        outputs:
            the outputs from the iterating node are appended to an iteration list, hence they have '_list' appended to their name
    """
    def __init__(self, conditional_node : ProcessNode, conditional_input : str, default_condition : bool = None, input_output_map : tuple[tuple[NodeInput, NodeOutput]] = None, description : str = "") -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        # save arguments
        assert isinstance(conditional_node, ProcessNode)
        self._conditional_node = conditional_node
        assert isinstance(conditional_input, str)
        self._conditional_input = conditional_input
    
        # create input output variables variables
        if default_condition is None:
            self._mandatory_inputs = (conditional_input, ) + conditional_node.mandatory_inputs
            self._non_mandatory_inputs = conditional_node.non_mandatory_inputs
            self._default_inputs = conditional_node.default_inputs
        else:
            self._mandatory_inputs = conditional_node.mandatory_inputs
            self._non_mandatory_inputs = (conditional_input, ) + conditional_node.non_mandatory_inputs
            self._default_inputs = (default_condition, ) + conditional_node.default_inputs
        self._outputs = conditional_node.outputs
        
        
        # check output negative condition
        in_out_map = [None]*len(self._outputs)
        if input_output_map is not None:
            for (input, output) in input_output_map:
                # check output
                assert isinstance(output, NodeOutput) and output.owner == conditional_node, f"Can only map a value to a NodeOutput of the conditional node."
                # check input
                if isinstance(input, NodeInput):
                    assert input.owner == conditional_node, f"Can only map a NodeInput of the conditional node."
                # update map
                in_out_map[self._outputs.index(output.name)] = input
        self._in_out_map = tuple(in_out_map)
        
        # create attributes
        self._createInputOutputAttributes()

    @property
    def outputs(self) -> tuple:
        return self._outputs

    def _run(self, **input_dict):
        if input_dict[self._conditional_input]:
            conditional_node_input = input_dict.copy()
            conditional_node_input.pop(self._conditional_input)
            return self._conditional_node.run(**conditional_node_input).values
        else:
            return tuple(input_dict[input.name] if isinstance(input, NodeInput) else input for input in self._in_out_map)

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
    