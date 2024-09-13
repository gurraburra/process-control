from multiprocess import cpu_count, Process, Pipe, Queue
from tqdm.auto import tqdm
import numpy as np
from itertools import chain
from collections.abc import Iterable
from ._ProcessNode import ProcessNode, NodeInput, NodeOutput
from ._NodeMappings import NodeInputOutput
from threading import Thread
import sys

def is_numeric(obj) -> bool:
    attrs = ['__add__', '__sub__', '__mul__', '__truediv__', '__pow__']
    return all(hasattr(obj, attr) for attr in attrs)

class RecursiveNode(ProcessNode):
    """
    An recursive node which iterates an internal node a certain number of times or until a condition is meet.

    Argument:
    ---------
    recursive_node: 
        the node to be recursively called
    break_recursion_node_output
        optional node outout which should determine when to break the recursion
        if not given a default node which ticks down a counter is used
    recursive_map: 
        the map between outputs and inputs of the recursive node
    """
    def __init__(self, recursive_node : ProcessNode, break_recursion_node_output : NodeOutput = None, recursive_map : tuple[tuple[NodeInputOutput, NodeInput]] = None, description : str = "") -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        # check recursive node
        assert isinstance(recursive_node, ProcessNode), f"recursive node needs to of type 'ProcessNode'"
        self._recursive_node = recursive_node

        # create recursive input map
        self._recursive_input_map = {}
        # check if break recursion node is none, create a default tick down node
        if break_recursion_node_output is None:
            # recursion node
            self._break_recursion_node = self._tickDownRecursion()
            self._break_recursion_node_output = self._break_recursion_node.output.stop
            # update recusive input map
            self._addRecursiveInput(self._break_recursion_node.output.nr_recursion, self._break_recursion_node.input.nr_recursion)
        else:
            # check type
            assert isinstance(break_recursion_node_output, NodeOutput), f"'break_recursion_node_output' needs to a 'NodeOutput', instead a '{type(break_recursion_node_output).__name__}' was given"
            self._break_recursion_node = break_recursion_node_output.owner
            self._break_recursion_node_output = break_recursion_node_output
        
        
        if recursive_map is not None:
            # check recursive map
            for node_input_output, input in recursive_map:
                # check output (can either be a an input or output)
                assert isinstance(node_input_output, NodeInputOutput), f"incorrect recursive mapping, node_input_output needs to a 'NodeInputOutput', instead a '{type(node_input_output).__name__}' was given"
                assert node_input_output.owner is self._recursive_node or node_input_output.owner is self._break_recursion_node, f"incorrect recursive mapping, owner of node_input_output needs to be the recursive or the break recursion node"
                # check input
                assert isinstance(input, NodeInput), f"Incorrect recursive mapping, input needs to a 'NodeInput', instead a '{type(input).__name__}' was given"
                assert input.owner is self._recursive_node or input.owner is self._break_recursion_node, f"incorrect recursive mapping, owner of input needs to be the recursive or the break recursion node"
                # update recusive input map
                self._addRecursiveInput(node_input_output, input)
        else:
            # all outputs with same name as input are interpreted as recursive outputs/inputs
            for output in self._recursive_node.outputs:
                # check if output exists among inputs
                if output in self._recursive_node.inputs:
                    # update recusive input map
                    self._addRecursiveInput(self._recursive_node.output[output], self._recursive_node.input[output])
            # check break recursion node (if break recursion node is None or recursive node, than recursive mapping already added)
            if break_recursion_node_output is not None and self._break_recursion_node is not self._recursive_node:
                for output in self._break_recursion_node.outputs:
                    # check if output exists among inputs
                    if output in self._break_recursion_node.inputs:
                        # update recusive input map
                        self._addRecursiveInput(self._break_recursion_node.output[output], self._break_recursion_node.input[node_input_output])
        # check if recursive node and break recursion node has recursion inputs
        recursion_nodes = set(input.owner for input in self._recursive_input_map)
        if self._recursive_node not in recursion_nodes:
            raise ValueError("no recursive inputs found for 'recursive_node'")
        if self._break_recursion_node not in recursion_nodes:
            raise ValueError("no recursive inputs found for 'break_recursion_node'")
        # add missing inputs for recursive node
        for input in self._recursive_node.input.values():
            if input not in self._recursive_input_map:
                self._recursive_input_map[input] = None
        # add missing inputs for break recusion node
        for input in self._break_recursion_node.input.values():
            if input not in self._recursive_input_map:
                self._recursive_input_map[input] = None
        
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
        
        # convert to tuple
        self._mandatory_inputs = tuple(self._mandatory_inputs)
        self._non_mandatory_inputs = tuple(self._non_mandatory_inputs)
        self._default_inputs = tuple(self._default_inputs)

        # outputs 
        self._outputs = tuple(f"Recusive_{output}" for output in self._recursive_node.outputs)
        if self._recursive_node is not self._break_recursion_node:
            self._outputs += tuple(f"Break_{output}" for output in self._break_recursion_node.outputs)
        self._outputs += ("__nr_recursive_runs__",)
        # # check no duplicate 
        # if len(outputs) != len(set(outputs)):
        #     raise ValueError(f"recursive and break recursive node share outputs")
        # else:
        #     self._outputs = outputs

        # create attributes
        self._createInputOutputAttributes()

    def _run(self, verbose : bool, **input_dict):
        # create inputs/outputs
        run_output = {self._recursive_node : {output : None for output in self._recursive_node.outputs}}
        run_input = {self._recursive_node : {inputs : None for inputs in self._recursive_node.inputs}}
        # if recursive and break node not the same
        if self._recursive_node is not self._break_recursion_node:
            run_output.update({self._break_recursion_node : {output : None for output in self._break_recursion_node.outputs}})
            run_input.update({self._break_recursion_node : {input : None for input in self._break_recursion_node.inputs}})    
        # update recursive outputs with inputs in case recursive node nevers gets to run
        for rec_input, rec_input_output in self._recursive_input_map.items():
            if isinstance(rec_input_output, NodeOutput):
                run_output[rec_input_output.owner][rec_input_output.name] = input_dict[self._retrieveInputName(rec_input)]
                run_input[rec_input.owner][rec_input.name] = rec_input_output._getData(input_dict[self._retrieveInputName(rec_input)])
            else:
                run_input[rec_input.owner][rec_input.name] = input_dict[self._retrieveInputName(rec_input)]
        # check verbose
        if verbose:
            pbar = tqdm(desc = f"{self}", miniters = 1)
        else:
            pbar = None
        # set nr recusrive runs to 0
        __nr_recursive_runs__ = 0
        while True:
            # create input to run break recursion node
            input_break_node = run_input[self._break_recursion_node] # {input.name : input_dict[self._retrieveInputName(input)] for input in self._break_recursion_node.input.values()}
            # run break recursion node
            break_run_output = self._break_recursion_node.run(verbose=verbose, **input_break_node)
            # update output from break recursion node
            for b_output in break_run_output.keys():
                run_output[self._break_recursion_node][b_output] = break_run_output[b_output]
            # check if recursion should stop
            if break_run_output[self._break_recursion_node_output.name]:
                break
            # check if recursive node is different from break node
            elif self._recursive_node is not self._break_recursion_node:
                # create input for recursive node
                input_rec_node = run_input[self._recursive_node] #{input.name : input_dict[self._retrieveInputName(input)] for input in self._recursive_node.input.values()}
                # run recursive node
                recursive_run_output = self._recursive_node.run(verbose=verbose, **input_rec_node)
                for r_output in recursive_run_output.keys():
                    run_output[self._recursive_node][r_output] = recursive_run_output[r_output]
            # update input dict
            old_input_dict = input_dict.copy()
            for rec_input, rec_input_output in self._recursive_input_map.items():
                if isinstance(rec_input_output, NodeOutput):
                    input_dict[self._retrieveInputName(rec_input)] = run_output[rec_input_output.owner][rec_input_output.name]
                elif isinstance(rec_input_output, NodeInput):
                    input_dict[self._retrieveInputName(rec_input)] = old_input_dict[self._retrieveInputName(rec_input_output)]
                else: # is None -> do nothing
                    pass
            # update inputs to nodes
            for rec_input, rec_input_output in self._recursive_input_map.items():
                if isinstance(rec_input_output, NodeOutput):
                    run_input[rec_input.owner][rec_input.name] = rec_input_output._getData(input_dict[self._retrieveInputName(rec_input)])
                elif isinstance(rec_input_output, NodeInput):
                    run_input[rec_input.owner][rec_input.name] = input_dict[self._retrieveInputName(rec_input)]
                else: # is None -> do nothing
                    pass
            # update nr recursvie runs and pbar
            __nr_recursive_runs__ += 1
            if pbar is not None:
                pbar.update()
        # close pbar
        if pbar is not None:
            pbar.close()
        # return result
        result = tuple(run_output[self._recursive_node][output] for output in self._recursive_node.outputs)
        if self._recursive_node is not self._break_recursion_node:
            result += tuple(run_output[self._break_recursion_node][output] for output in self._break_recursion_node.outputs)
        result += (__nr_recursive_runs__, )
        return result

    def _addRecursiveInput(self, node_input_output : NodeInputOutput, input : NodeInput):
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

    class _tickDownRecursion(ProcessNode):
        outputs = ("stop", "nr_recursion")
    
        def _run(self, nr_recursion):
            if nr_recursion <= 0:
                return True, 0
            else:
                return False, nr_recursion - 1

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