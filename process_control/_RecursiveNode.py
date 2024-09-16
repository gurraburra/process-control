from tqdm.auto import tqdm
from ._ProcessNode import ProcessNode, NodeInput, NodeOutput
from ._NodeMappings import NodeInputOutput
import warnings

def is_numeric(obj) -> bool:
    attrs = ['__add__', '__sub__', '__mul__', '__truediv__', '__pow__']
    return all(hasattr(obj, attr) for attr in attrs)

class RecursiveNode(ProcessNode):
    """
    An recursive node which iterates an internal node a certain number of times or until a condition is meet.

    Argument:
    ---------
    recursive_node: 
        the node to be recursively called, the node have access to the
    break_recursion_node_output
        optional node output which should determine when to break the recursion
        if not given a default node which ticks down a counter is used
        can also be an output from the recursive node itself
    recursive_input_map: 
        the map between outputs and inputs of the recursive node
    Note: 
        both nodes have access to the variable '__nr_recursive_runs__' which is 
        updated after each recursive run by simply adding it as an input
    """
    def __init__(self, recursive_node : ProcessNode, break_recursion_node_output : NodeOutput = None, recursive_map : tuple[tuple[NodeInput,NodeInputOutput]] = None, include_break_recursion_output : bool = False, description : str = "") -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        # save arguments
        self._include_break_recursion_output = include_break_recursion_output
        # check recursive node
        assert isinstance(recursive_node, ProcessNode), f"recursive node needs to of type 'ProcessNode'"
        self._recursive_node = recursive_node

        # check if break recursion node is none, create a default tick down node
        if break_recursion_node_output is None:
            # recursion node
            self._break_recursion_node = self._nrRecursion()
            self._break_recursion_node_output = self._break_recursion_node.output.stop
            # update recusive input map
            # self._addRecursiveInput(self._break_recursion_node.input.nr_recursion, self._break_recursion_node.output.nr_recursion)
        else:
            # check type
            assert isinstance(break_recursion_node_output, NodeOutput), f"'break_recursion_node_output' needs to a 'NodeOutput', instead a '{type(break_recursion_node_output).__name__}' was given"
            self._break_recursion_node = break_recursion_node_output.owner
            self._break_recursion_node_output = break_recursion_node_output
        
        # create recursive input map
        self._recursive_input_map = {}
        if recursive_map is not None:
            # check recursive map
            for input, node_input_output in recursive_map:
                # check output (can either be a an input or output)
                assert isinstance(node_input_output, NodeInputOutput), f"incorrect recursive mapping, node_input_output needs to a 'NodeInputOutput', instead a '{type(node_input_output).__name__}' was given"
                assert node_input_output.owner is self._recursive_node or node_input_output.owner is self._break_recursion_node, f"incorrect recursive mapping, owner of node_input_output needs to be the recursive or the break recursion node"
                # check input
                assert isinstance(input, NodeInput), f"Incorrect recursive mapping, input needs to a 'NodeInput', instead a '{type(input).__name__}' was given"
                assert input.owner is self._recursive_node or input.owner is self._break_recursion_node, f"incorrect recursive mapping, owner of input needs to be the recursive or the break recursion node"
                # update recusive input map
                self._addRecursiveInput(input, node_input_output)
        else:
            # all outputs with same name as input are interpreted as recursive outputs/inputs
            for output in self._recursive_node.outputs:
                # check if output exists among inputs
                if output in self._recursive_node.inputs:
                    # update recusive input map
                    self._addRecursiveInput(self._recursive_node.input[output], self._recursive_node.output[output])
            # check break recursion node (if break recursion node is None or recursive node, than recursive mapping already added)
            if break_recursion_node_output is not None and self._break_recursion_node is not self._recursive_node:
                for output in self._break_recursion_node.outputs:
                    # check if output exists among inputs
                    if output in self._break_recursion_node.inputs:
                        # update recusive input map
                        self._addRecursiveInput(self._break_recursion_node.input[node_input_output], self._break_recursion_node.output[output])
        # check if nodes has input __nr_recursive_runs__
        tmp_output = NodeOutput(self, "__nr_recursive_runs__")
        if "__nr_recursive_runs__" in self._recursive_node.inputs:
            # add recursion map
            self._addRecursiveInput(self._recursive_node.input["__nr_recursive_runs__"], tmp_output)
        if self._break_recursion_node is not self._recursive_node and "__nr_recursive_runs__" in self._break_recursion_node.inputs:
            # add recursion map
            self._addRecursiveInput(self._break_recursion_node.input["__nr_recursive_runs__"], tmp_output)
        # check if recursive node and break recursion node has recursion inputs
        recursion_nodes = set(input.owner for input in self._recursive_input_map)
        # check recursive node has recursive inputs
        if self._recursive_node not in recursion_nodes:
            raise ValueError("no recursive inputs found for 'recursive_node'")
        # check break recursion node has recursive inputs
        if self._break_recursion_node not in recursion_nodes:
            raise ValueError("no recursive inputs found for 'break_recursion_node'")
        # add non recursive inputs for recursive node
        for input in self._recursive_node.input.values():
            if input not in self._recursive_input_map:
                self._addRecursiveInput(input, None)
        # add non recursive inputs for break recusion node
        for input in self._break_recursion_node.input.values():
            if input not in self._recursive_input_map:
                self._addRecursiveInput(input, None)
        # check for redudant recursion mapping
        def isRecursive(input):
            rec_input_output = self._recursive_input_map[input]
            if isinstance(rec_input_output, NodeOutput):
                return True
            elif isinstance(rec_input_output, NodeInput):
                return isRecursive(rec_input_output)
            else:
                return False
        for rec_input, rec_input_output in self._recursive_input_map.items():
            # mapped to node output -> recursive
            if isinstance(rec_input_output, NodeOutput):
                continue
            # mapped to another node input
            elif isinstance(rec_input_output, NodeInput):
                # check if recursive
                if not isRecursive(rec_input_output):
                    raise ValueError(f"input '{rec_input}' is mapped to a non recursive input")
            else: # not mapped
                continue
        
        # create input attributes
        self._mandatory_inputs = []
        self._non_mandatory_inputs = []
        self._default_inputs = []
        # conflicting non mandatory inputs
        conflicting_non_mandatory_input = []
        # loop through recursive and break recursion node
        for node in [self._recursive_node, self._break_recursion_node]:
            # check first mandatory inputs
            for mand_input in node.mandatory_inputs:
                # get recursive input name
                mand_input = self._retrieveInputName(node.input[mand_input])
                # if mandatory input exist as mandatory input -> leave it there
                if mand_input in self._mandatory_inputs:
                    pass
                # check if mandatory input exist in non mandatory inputs
                elif mand_input in self._non_mandatory_inputs:
                    # leave it there
                    pass
                # if not previously added -> add it now
                else:
                    self._mandatory_inputs.append(mand_input)
            # check non mandatory inputs and default values
            for non_mand_input, default_input in zip(node.non_mandatory_inputs, node.default_inputs):
                # get recursive input name
                non_mand_input = self._retrieveInputName(node.input[non_mand_input])
                # if non mandatory input exist as mandatory input -> leave it there
                if non_mand_input in self._mandatory_inputs:
                    # change to non mandatory if not in conflicting
                    if non_mand_input not in conflicting_non_mandatory_input:
                        idx_cond = self._mandatory_inputs.index(non_mand_input)
                        del self._mandatory_inputs[idx_cond]
                        self._non_mandatory_inputs.append(non_mand_input)
                        self._default_inputs.append(default_input)
                    else:
                        # leave it there
                        pass
                # if in non mandatory input -> check if default value is same, otherwise change to mandatory
                elif non_mand_input in self._non_mandatory_inputs:
                    idx_cond = self._non_mandatory_inputs.index(non_mand_input)
                    if self._default_inputs[idx_cond] != default_input:
                        # change it to mandatory
                        idx_cond = self._non_mandatory_inputs.index(non_mand_input)
                        del self._non_mandatory_inputs[idx_cond]
                        del self._default_inputs[idx_cond]
                        self._mandatory_inputs.append(non_mand_input)
                        # update conflicting list
                        conflicting_non_mandatory_input.append(non_mand_input)
                # if not in either mandatory or non mandatory inputs -> add it to non mandatory inputs
                else:
                    self._non_mandatory_inputs.append(non_mand_input)
                    self._default_inputs.append(default_input)
        
        # check if __nr_recursive_runs__ in inputs, then remove since its provided by self
        if '__nr_recursive_runs__' in self._mandatory_inputs:
            idx_cond = self._mandatory_inputs.index('__nr_recursive_runs__')
            del self._mandatory_inputs[idx_cond]
        elif '__nr_recursive_runs__' in self._mandatory_inputs:
            idx_cond = self._non_mandatory_inputs.index('__nr_recursive_runs__')
            del self._non_mandatory_inputs[idx_cond]
            del self._default_inputs[idx_cond]

        # convert to tuple
        self._mandatory_inputs = tuple(self._mandatory_inputs)
        self._non_mandatory_inputs = tuple(self._non_mandatory_inputs)
        self._default_inputs = tuple(self._default_inputs)

        # outputs -> add all as long as not break recursion node output
        self._outputs = tuple(f"Recursive_{output.name}" for output in self._recursive_node.output.values() if not NodeInputOutput.__eq__(output, self._break_recursion_node_output))
        if self._recursive_node is not self._break_recursion_node and self._include_break_recursion_output:
            self._outputs += tuple(f"Break_{output.name}" for output in self._break_recursion_node.output.values() if not NodeInputOutput.__eq__(output, self._break_recursion_node_output))
        self._outputs += (tmp_output.name,)

        # create attributes
        self._createInputOutputAttributes()

    def _run(self, verbose : bool, **input_dict):
        # Architectures
        # input_dict -> recursive_input_map -> node_input -> node.run(...) -> node_output -> input_dict -> ...
        # add nr recursive runs to input since its a recursive output coming from self
        input_dict['__nr_recursive_runs__'] = 0
        # create run inputs/outputs 
        node_output = {self._break_recursion_node : {output.name : None for output in self._break_recursion_node.output.values() if not NodeInputOutput.__eq__(output, self._break_recursion_node_output)}}
        node_input = {self._break_recursion_node : {input : None for input in self._break_recursion_node.inputs}} 
        # if recursive and break node not the same
        if self._recursive_node is not self._break_recursion_node:
            node_output.update({self._recursive_node : {output.name : None for output in self._recursive_node.output.values() if not NodeInputOutput.__eq__(output, self._break_recursion_node_output)}})
            node_input.update({self._recursive_node : {inputs : None for inputs in self._recursive_node.inputs}})
        # add output from self
        node_output.update({self : {"__nr_recursive_runs__" : 0}})
        # update recursive outputs with inputs in case recursive node nevers gets to run
        for rec_input, rec_input_output in self._recursive_input_map.items():
            if isinstance(rec_input_output, NodeOutput):
                node_output[rec_input_output.owner][rec_input_output.name] = input_dict[self._retrieveInputName(rec_input)]
                node_input[rec_input.owner][rec_input.name] = rec_input_output._getData(input_dict[self._retrieveInputName(rec_input)])
            else:
                node_input[rec_input.owner][rec_input.name] = input_dict[self._retrieveInputName(rec_input)]
        # check verbose
        if verbose:
            if isinstance(self._break_recursion_node, self._nrRecursion):
                total = input_dict['nr_recursion']
            else:
                total = None
            pbar = tqdm(desc = f"{self}", miniters = 1, total = total)
        else:
            pbar = None
        while True:
            # create input to run break recursion node
            input_break_node = node_input[self._break_recursion_node] # {input.name : input_dict[self._retrieveInputName(input)] for input in self._break_recursion_node.input.values()}
            # run break recursion node
            break_run_output = self._break_recursion_node.run(verbose=verbose, **input_break_node)
            # check if recursion should stop
            if self._break_recursion_node_output._getData(break_run_output[self._break_recursion_node_output.name]):
                # break before updating the outputs since the previous outputs should be used
                break
            else:
                # update output from break recursion node
                for b_output in break_run_output.keys() / self._break_recursion_node_output.name:
                    node_output[self._break_recursion_node][b_output] = break_run_output[b_output]
                # check if recursive node is different from break node
                if self._recursive_node is not self._break_recursion_node:
                    # create input for recursive node
                    input_rec_node = node_input[self._recursive_node] #{input.name : input_dict[self._retrieveInputName(input)] for input in self._recursive_node.input.values()}
                    # run recursive node
                    recursive_run_output = self._recursive_node.run(verbose=verbose, **input_rec_node)
                    for r_output in recursive_run_output.keys():
                        node_output[self._recursive_node][r_output] = recursive_run_output[r_output]
            # update output from self
            node_output[self]['__nr_recursive_runs__'] += 1
            # update pbar
            if pbar is not None:
                pbar.update()
            # update input dict
            old_input_dict = input_dict.copy()
            for rec_input, rec_input_output in self._recursive_input_map.items():
                if isinstance(rec_input_output, NodeOutput):
                    input_dict[self._retrieveInputName(rec_input)] = node_output[rec_input_output.owner][rec_input_output.name]
                elif isinstance(rec_input_output, NodeInput):
                    input_dict[self._retrieveInputName(rec_input)] = old_input_dict[self._retrieveInputName(rec_input_output)]
                else: # is None -> do nothing
                    pass
            # update inputs to nodes
            for rec_input, rec_input_output in self._recursive_input_map.items():
                if isinstance(rec_input_output, NodeOutput):
                    node_input[rec_input.owner][rec_input.name] = rec_input_output._getData(input_dict[self._retrieveInputName(rec_input)])
                elif isinstance(rec_input_output, NodeInput):
                    node_input[rec_input.owner][rec_input.name] = input_dict[self._retrieveInputName(rec_input)]
                else: # is None -> pass
                    pass
        # close pbar
        if pbar is not None:
            pbar.close()
        # return result
        result = tuple(node_output[self._recursive_node].values())
        if self._recursive_node is not self._break_recursion_node and self._include_break_recursion_output:
            result += tuple(node_output[self._break_recursion_node].values())
        result += (node_output[self]['__nr_recursive_runs__'], )
        return result

    def _addRecursiveInput(self, input : NodeInput, node_input_output : NodeInputOutput):
        # update internal map ( check if recursion already added )
        if input not in self._recursive_input_map:
            self._recursive_input_map[input] = node_input_output
        else: 
            raise ValueError(f"double recursive mapping found for input: '{input}'")
    
    def _retrieveInputName(self, input : NodeInput):
        # check if input recursive mapped to an output
        if isinstance(self._recursive_input_map[input], NodeOutput):
            # return outputs name
            return self._recursive_input_map[input].name
        # check if recursively mapped to an input
        elif isinstance(self._recursive_input_map[input], NodeInput):
            # get name of that input
            return "Prev_" + self._retrieveInputName(self._recursive_input_map[input])
        # if not mapped
        else:
            # return its own name
            return input.name

    class _nrRecursion(ProcessNode):
        outputs = ("stop",)
    
        def _run(self, nr_recursion : int, __nr_recursive_runs__ : int):
            return __nr_recursive_runs__ >= nr_recursion, 

    @property
    def outputs(self) -> tuple:
        return self._outputs

    # new properties
    @property
    def recursive_node(self) -> ProcessNode:
        return self._recursive_node
    
    # modified properties
    @property
    def class_inputs(self) -> tuple:
        return self.mandatory_inputs + self.non_mandatory_inputs
    
    @property
    def class_mandatory_inputs(self) -> tuple:
        return self._mandatory_inputs
    
    @property
    def class_non_mandatory_inputs(self) -> tuple:
        return self._non_mandatory_inputs
    
    @property
    def class_default_inputs(self) -> tuple:
        return self._default_inputs