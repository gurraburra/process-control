from ._ProcessNode import ProcessNode, NodeInput, NodeOutput

class CondtionalNode(ProcessNode):
    """
    A conditioanl node will only run internal node if condition is True.

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
    def __init__(self, conditional_node : ProcessNode, conditional_input : str, default_condition : bool = None, output_map_negative_condition : tuple[tuple[NodeInput, NodeOutput]] = None, description : str = "") -> None:
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
        output_map_neg = [None,]*len(self._outputs)
        if output_map_negative_condition is not None:
            for (input, output) in output_map_negative_condition:
                assert isinstance(input, NodeInput), f"Negative condition can only map input to output of the conditional node."
                assert isinstance(output, NodeOutput), f"Negative condition can only map input to output of the conditional node."
                assert input.owner == conditional_node, f"Negative condition can only map input to output of the conditional node."
                assert output.owner == conditional_node, f"Negative condition can only map input to output of the conditional node."
            output_map_neg[self._outputs.index(output.name)] = input
        self._output_map_neg = tuple(output_map_neg)
        
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
            return tuple(input_dict[out.name] if isinstance(out, NodeInput) else out for out in self._output_map_neg)

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
    