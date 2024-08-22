import re
import numpy as np
from ._ProcessNode import ProcessNode, _BinaryOperand, NodeOutput, NodeInput
from ._NodeMappings import NodeRunOutput,  NodeMapping
from ._SimpleNodes import ValueNode

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
    
    
    def _run(self, ignore_cache : bool, update_cache : bool, verbose : bool, **input_dict) -> tuple:
        # check nodes intialized
        if not self._nodes_init:
            raise RuntimeError(f"Nodes of workflow has not been initalized.")
        # create input data transfer
        input_data_transfer = {node : {} for node in self._internal_nodes}
        # tranfer this workflow inputs to its internal nodes
        self._transferNodeOutputs(NodeRunOutput(self, tuple(input_dict.keys()), tuple(input_dict.values())), input_data_transfer)
        
        # loop through all execution order
        for order in range(1, max(self.execution_order, default=-1) + 1):
            self._executeNodes(
                    tuple(node_idx for node_idx, node_order in enumerate(self.execution_order) if node_order == order), 
                        input_data_transfer, ignore_cache=ignore_cache, update_cache=update_cache, verbose=verbose)

        # return workflow inputs from its internal nodes
        return tuple(input_data_transfer[self][output] for output in self.outputs)


    def _executeNodes(self, node_idxs : tuple[int], input_data_transfer : dict, ignore_cache : bool, update_cache : bool, verbose : bool):
        for node_idx in node_idxs:
            node = self.nodes[node_idx]
            self._transferNodeOutputs(node.run(ignore_cache=ignore_cache, update_cache=update_cache, verbose=verbose, **input_data_transfer[node]), input_data_transfer)

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
            # # check for node = None, change to self
            # if isinstance(output, str):
            #     # split string in output name and attribute parts -> this allows for manipulating input to workflow
            #     output_parts = re.split('([^a-zA-Z_])', output.replace(" ",""), 1)
            #     output = eval('NodeOutput(self, output_parts[0])' + ''.join(output_parts[1:]))
            # if isinstance(input, str):
            #     input = NodeInput(self, input.replace(" ",""))
            # assert output is NodeOutut
            # assert isinstance(output, NodeOutput), f"Incorrect output defined in entry #{i}"
            # # check if output is workflow output
            # if output.owner is None:
            #     # then update owner
            #     output.owner = self
            # check if tuple (an one to many mapping)
            if isinstance(input, (tuple,list)):
                # assert isinstance(output, NodeOutput), f"Incorrect output defined in entry #{i}"
                # if output is not a NodeOutput -> wrap it in an ValueNode
                if not isinstance(output, NodeOutput):
                    output = ValueNode(output).output.value
                # check for blank output name, only allowed in output is this workflow, signal by owner is None
                if output.name is None:
                    assert output.owner is None, f"Expected a workflow output in entry #{i}"
                    for in_ in input:
                        assert isinstance(in_, NodeInput), f"Incorrect input defined in entry #{i}"
                        assert in_.name is not None, f"Blank to blank mapping found in entry #{i}"
                        # copy output so all attributes comes with
                        out_ = output.copy()
                        out_.name = in_.name
                        # add to inputs outputs
                        outputs.append(out_)
                        inputs.append(in_)
                else:
                    for in_ in input:
                        assert isinstance(in_, NodeInput), f"Incorrect input defined in entry #{i}"
                        # check for blank input
                        if in_.name is None:
                            assert in_.owner is None, f"Incorrect input defined in entry #{i}"
                            in_ = NodeInput(None, output.name)
                        outputs.append(output)
                        inputs.append(in_)
            # check if tuple (an 'all' mapping)
            elif isinstance(output, (tuple,list)):
                # check input is a blank workflow input
                assert input.owner is None, f"Expected a workflow input in entry #{i}"  
                assert input.name is None, f"Incorrect input defined in entry #{i}"

                for out_ in output:
                    # assert isinstance(out_, NodeOutput), f"Incorrect output defined in entry #{i}"
                    # if output is not a NodeOutput -> wrap it in an ValueNode
                    if not isinstance(out_, NodeOutput):
                        out_ = ValueNode(out_).output.value
                    assert out_.name is not None, f"Blank to blank mapping found in entry #{i}"
                    inputs.append(NodeInput(None, out_.name))
                    outputs.append(out_)
            else:
                # assert types
                # assert isinstance(output, NodeOutput), f"Incorrect output defined in entry #{i}"
                # if output is not a NodeOutput -> wrap it in an ValueNode
                if not isinstance(output, NodeOutput):
                    output = ValueNode(output).output.value
                assert isinstance(input, NodeInput), f"Incorrect input defined in entry #{i}"
                # check for empty output or input strings
                if output.owner is None and output.name is None and input.name is not None:
                    output.name = input.name
                if input.owner is None and input.name is None and output.name is not None:
                    input.name = output.name
                
                # check for empty input output, not allowed unless mapped to workflow (handled above)
                if output.name is None or input.name is None:
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
                # handle input 2
                outputs.append(binary_output.owner.input_2)
                inputs.append(binary_output.owner.input.input_2)
                # don't need the two lines below since appending new inputs 
                # to the outputs list will make sure they are handled later
                # addOutputsInputsBinaryOperand(binary_output.owner.input_1)
                # addOutputsInputsBinaryOperand(binary_output.owner.input_2)
        for output in outputs:
            addOutputsInputsBinaryOperand(output)

        # update owner of workflow inputs outputs, marked with owner = None
        for output, input in zip(outputs, inputs):
            if output.owner is None:
                output.owner = self
            if input.owner is None:
                input.owner = self
        
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
        input_output_wf = {"mandatory_inputs" : [], "non_mandatory_inputs" : [], "default_inputs" : [], "conflicting_non_mandatory_inputs" : []}
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
        self._mandatory_inputs = tuple(input_output_wf["mandatory_inputs"])
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
                    output_str not in input_output_wf["mandatory_inputs"]:
                    # store as mandatory mapping for now
                    input_output_wf["mandatory_inputs"].append(output_str)
            # otherwise node_in is internal
            else:
                # check if output already assigned as non mandatory inputs
                if output_str in input_output_wf["non_mandatory_inputs"]:
                    # check if input is non mandatory in node_in
                    if input_str in input_node.non_mandatory_inputs:
                        # get index
                        idx = input_output_wf["non_mandatory_inputs"].index(output_str)
                        # if yes, check if default values are not the same
                        if input_output_wf["default_inputs"][idx] != input_node.default_inputs[input_node.non_mandatory_inputs.index(input_str)]:
                            # if not change input to mandatory
                            del input_output_wf["non_mandatory_inputs"][idx]
                            del input_output_wf["default_inputs"][idx]
                            input_output_wf["mandatory_inputs"].append(output_str)
                            # remeber the conflict for future reference
                            input_output_wf["conflicting_non_mandatory_inputs"].append(output_str)
                    # if input is mandatory in node_in
                    else:
                        # leave in non mandatory
                        pass
                elif output_str in input_output_wf["mandatory_inputs"]:
                    # check output not earlier removed from non mandatory input due to conflict and if input is non mandatory in node_in
                    if output_str not in input_output_wf["conflicting_non_mandatory_inputs"] and input_str in input_node.non_mandatory_inputs:
                        # change input to non mandatory
                        idx = input_output_wf["mandatory_inputs"].index(output_str)
                        del input_output_wf["mandatory_inputs"][idx]
                        # store as non-mandatory input
                        input_output_wf["non_mandatory_inputs"].append(output_str)
                        # store default input
                        input_output_wf["default_inputs"].append(input_node.default_inputs[input_node.non_mandatory_inputs.index(input_str)])
                # else if it has not been added yet
                else:
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
            raise ValueError(f"Cycle dependency found for node {node_idx+1} ({self._internal_nodes[node_idx]})")
        
    # input output to workflow to be used during defintion of the workflow
    class WfMapping(NodeMapping):
        def __init__(self, is_wf_input: bool) -> None:
            super().__init__(None, [], [], is_wf_input)
            self._is_wf_input = is_wf_input

        def __getattr__(self, key):
            if isinstance(key, str) and key.isidentifier():
                # check for blank input output, marked with '_'
                if key == "_":
                    name = None
                else:
                    name = key
                if self._is_wf_input:
                    # an input to the workflow is seen internally as an output
                    # since data comes from the 'outside' and is immediately transfered to internal nodes
                    return NodeOutput(None, name)
                else:
                    # an output of the workflow is seen internally as an input
                    # since data comes from the internal nodes before being transfered to the 'ouside'
                    return NodeInput(None, name)
            else:
                raise ValueError(f"Incorrect workflow input/output identifier: {key}.")
        def __str__(self) -> str:
            return f"Workflow {self._input_output_str}"
    
    input = WfMapping(True)
    output = WfMapping(False)

    def cacheNodesData(self, val : bool) -> None:
        for n in self.nodes:
            n.cache_data = bool(val)

    def cacheNodesInput(self, val : bool) -> None:
        for n in self.nodes:
            n.cache_input = bool(val)

    def cacheNodesOutput(self, val : bool) -> None:
        for n in self.nodes:
            n.cache_output = bool(val)
        
    def resetNodesCache(self) -> None:
        for n in self.nodes:
            n.resetCache()
    