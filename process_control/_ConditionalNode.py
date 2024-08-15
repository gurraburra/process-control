from ._ProcessNode import ProcessNode, NodeInput, NodeOutput

class ConditionalNode(ProcessNode):
    """
    A conditioanl node will only run internal node if condition is True.

    Argument:
    ---------
    conditional_input: 
        the condition to determine which node to run
    condition_node_map: 
        map from condition to node, node can be set do 'None' which is interpret as 'do nothing if this condition is given'
    default_condition: 
        default condition if the conditional_input not given at runtime
    """
    # no default value
    __no_default_condition = None
    
    def __init__(self, conditional_input : str, condition_node_map : dict[object, ProcessNode], default_condition : object = __no_default_condition, input_mapping : dict[str, tuple[NodeInput]] = None, output_mapping : dict[str, tuple[NodeOutput]] = None, description : str = "") -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        # save arguments
        # check conditional input
        assert isinstance(conditional_input, str)
        self._conditional_input = conditional_input
        # check condition node map
        assert isinstance(condition_node_map, dict)
        for node in condition_node_map.values():
            if node is not None: 
                assert isinstance(node, ProcessNode)
        self._condition_node_map = condition_node_map
        # check default input
        if default_condition is not self.__no_default_condition:
            assert default_condition in condition_node_map, f"Missing default condition: {default_condition}."
        self._default_condition = default_condition

        if output_mapping is None:
            self._internal_map_out = None
        else:
            # create internal map from nodes to outputs of conditional node
            internal_map_out = {node : {} for node in condition_node_map.values() if node is not None}
            # loop through output mapping
            for cond_output, node_outputs in output_mapping.items():
                # make sure they have right types
                assert isinstance(cond_output, str)
                # if node_outputs is just single node_output -> put in in a tuple
                if isinstance(node_outputs, NodeOutput):
                    node_outputs = (node_outputs, )
                assert isinstance(node_outputs, (list, tuple))
                # check each no double mapping
                assert len(node_outputs) == len(set([output.owner for output in node_outputs])), f"Double mappings given for output '{cond_output}'."
                # check each output
                for node_output in node_outputs:
                    assert isinstance(node_output, NodeOutput)
                    assert node_output.owner in internal_map_out, f"Incorrect node output {node_output}, node not among conditional nodes."
                    internal_map_out[node_output.owner][cond_output] = node_output.name

            # check all nodes has maps to outputs
            for node, mapping in internal_map_out.items():
                for cond_output in output_mapping.keys():
                    if cond_output not in mapping:
                        assert cond_output in node.outputs, f"Conditional node {node} is missing output '{cond_output}'."
                        internal_map_out[node][cond_output] = cond_output
            # save internal map
            self._internal_map_out = internal_map_out

        # check that all nodes produce same outputs
        outputs = []
        for node in condition_node_map.values():
            if node is not None:
                if self._internal_map_out is not None:
                    outputs.append(frozenset(self._internal_map_out[node].keys()))
                else:
                    outputs.append(node.outputs)
        nr_outputs = len(set(outputs))
        if nr_outputs > 1:
            raise ValueError("Conditional nodes contains different outputs.")
        elif nr_outputs == 0:
            raise ValueError("No outputs produced by conditional nodes.")
        else:
            self._outputs = tuple(outputs[0])


        # create input output variables variables
        if self._default_condition is self.__no_default_condition:
            self._mandatory_inputs = [conditional_input]
            self._non_mandatory_inputs = []
            self._default_inputs = []
        else:
            self._mandatory_inputs = []
            self._non_mandatory_inputs = [conditional_input]
            self._default_inputs = [self._default_condition]

        if input_mapping is None:
            self._internal_map_in = None
        else:
            # create internal map from nodes to outputs of conditional node
            internal_map_in = {node : {} for node in condition_node_map.values() if node is not None}
            # list with conditional node outputs
            inputs = []
            # loop through output mapping
            for cond_input, node_inputs in input_mapping.items():
                # make sure they have right types
                assert isinstance(cond_input, str)
                # if node_inputs is just single node_input -> put in in a tuple
                if isinstance(node_inputs, NodeInput):
                    node_inputs = (node_inputs, )
                assert isinstance(node_inputs, (list, tuple))
                # check each input
                for node_input in node_inputs:
                    assert isinstance(node_input, NodeInput)
                    assert node_input.owner in internal_map_in, f"Incorrect node input {node_input}, node not among conditional nodes."
                    internal_map_in[node_input.owner][node_input.name] = cond_input
            
            # check all nodes has maps for their inputs
            for node, mapping in internal_map_in.items():
                for input in node.inputs:
                    if input not in mapping:
                        mapping[input] = input
            # save internal map
            self._internal_map_in = internal_map_in

        # loop through inputs of all nodes in conditional mapping
        for node in condition_node_map.values():
            if node is not None:
                # check first mandatory inputs
                for mand_input in node.mandatory_inputs:
                    # check internal mapping
                    if self._internal_map_in is not None:
                        mand_input = self._internal_map_in[node][mand_input]
                    # if mandatory input exist as mandatory input -> leave it there
                    if mand_input in self._mandatory_inputs:
                        pass
                    # check if mandatory input exist in non mandatory inputs
                    elif mand_input in self._non_mandatory_inputs:
                        # change it to mandatory
                        idx_cond = self._non_mandatory_inputs.index(mand_input)
                        del self._non_mandatory_inputs[idx_cond]
                        del self._default_inputs[idx_cond]
                        self._mandatory_inputs.append(mand_input)
                    # if not previously added -> add it now
                    else:
                        self._mandatory_inputs.append(mand_input)
                # check non mandatory inputs and default values
                for non_mand_input, default_input in zip(node.non_mandatory_inputs, node.default_inputs):
                    # check internal mapping
                    if self._internal_map_in is not None:
                        non_mand_input = self._internal_map_in[node][non_mand_input]
                    # if non mandatory input exist as mandatory input -> leave it there
                    if non_mand_input in self._mandatory_inputs:
                        pass
                    # if in non mandatory input -> check if default value is same, otherwise notify user
                    elif non_mand_input in self._non_mandatory_inputs:
                        idx_cond = self._non_mandatory_inputs.index(non_mand_input)
                        if self._default_inputs[idx_cond] != f"--depends on condition '{self._conditional_input}'--" and self._default_inputs[idx_cond] != default_input:
                            self._default_inputs[idx_cond] = f"--depends on condition '{self._conditional_input}'--"
                            print(f"Different default values detected for input: {non_mand_input}.")
                    # if not in either mandatory or non mandatory inputs -> add it to non mandatory inputs
                    else:
                        self._non_mandatory_inputs.append(non_mand_input)
                        self._default_inputs.append(default_input)
        # convert to tuple
        self._mandatory_inputs = tuple(self._mandatory_inputs)
        self._non_mandatory_inputs = tuple(self._non_mandatory_inputs)
        self._default_inputs = tuple(self._default_inputs)
        
        # create attributes
        self._createInputOutputAttributes()

    @property
    def outputs(self) -> tuple:
        return self._outputs

    def _run(self, ignore_cache : bool, update_cache : bool, verbose : bool, **input_dict):
        # get condition
        condition = input_dict[self._conditional_input]
        # check for if condition is valid
        if condition not in self._condition_node_map:
            if self._default_condition is self.__no_default_condition:
                raise RuntimeError(f"Node {self} got unexpected condition: {condition}.")
            else:
                condition = self._default_condition
        # it it is get node to execute
        conditional_node = self._condition_node_map[condition]
        # allow for conditional node to be None -> Do nothing
        if conditional_node is not None:
            # create input_dict to conditional node
            conditional_node_input = {}
            # check if internal mapping exist
            if self._internal_map_in is not None:
                # make sure input_dict it only contains inputs to that specific nodes
                for input in conditional_node.inputs:
                    # get input to conditional node from mapping
                    cond_input = self._internal_map_in[conditional_node][input]
                    # add input if given
                    if cond_input in input_dict:
                        conditional_node_input[input] = input_dict[cond_input]
            else:
                # make sure input_dict it only contains inputs to that specific nodes
                for input in conditional_node.inputs:
                    # add input if given
                    if input in input_dict:
                        conditional_node_input[input] = input_dict[input]
            # run conditional node
            result = conditional_node.run(ignore_cache=ignore_cache, update_cache=update_cache, verbose=verbose, **conditional_node_input)
            # check if internal map out exist
            if self._internal_map_out is not None:
                # if yes -> use it to map output from node
                return tuple(result[self._internal_map_out[conditional_node][out]] for out in self.outputs)
            else:
                # if not -> make sure order of outputs corresponds to self.outputs
                return tuple(result[out] for out in self.outputs)
        else:
            return (None,) * len(self.outputs)
        
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
    
    # override check run inputs
    def _checkRunInputs(self, input_dict : dict) -> None:
        # check if condition_input is given if no default_condition exists
        if self._default_condition is self.__no_default_condition and self._conditional_input not in input_dict:
            raise TypeError(f"Node {self} is missing input '{self._conditional_input}'.")
        # check for redudant inputs
        for input_str in input_dict.keys():
            if input_str not in self.inputs:
                raise TypeError(f"Node {self} got an unexpected input '{input_str}'.")
            
    # override add non mandaotry inputs
    def _addNonMandatoryInputs(self, input_dict : dict) -> None:
        # add default_condition if condtional_input not given
        if self._conditional_input not in input_dict:
            input_dict[self._conditional_input] = self._default_condition