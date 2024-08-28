from multiprocess import cpu_count, Process, Pipe, Queue
from tqdm.auto import tqdm
import numpy as np
from itertools import chain
from collections.abc import Iterable
from ._ProcessNode import ProcessNode
from threading import Thread
import sys

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
    def __init__(self, iterating_node : ProcessNode, iterating_inputs : tuple[str], iterating_name : str = "", exclude_outputs : tuple[str] = tuple(), parallel_processing : bool = False, nr_processes : int = -1, show_pbar : bool = True, description : str = "") -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        # save arguments
        self._iterating_node = iterating_node
        self._iterating_name = iterating_name
        self.parallel_processing = parallel_processing
        self.nr_processes = nr_processes
        self.show_pbar = show_pbar
        
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

        # exclude outputs
        if isinstance(exclude_outputs, str):
            exclude_outputs = (exclude_outputs,)
        # check all exclude outputs is in interating node
        for exclude in exclude_outputs:
            if not exclude in iterating_node.output.keys():
                raise ValueError(f"Output {exclude} to exclude does not exist in node: {iterating_node}.")
        self.include_outputs = iterating_node.output.keys() - exclude_outputs

        # create outputs tuple and ad _list to all names
        self._outputs = tuple(self._listName(output) for output in self.include_outputs)
        # create attributes
        self._createInputOutputAttributes()

    @property
    def outputs(self) -> tuple:
        return self._outputs

    def _run(self, verbose : bool, nr_processes : int, **input_dict):
        # check input_dict
        nr_iter, moved_iterating_inputs = self._checkInput(input_dict)
        # handle zero iterations
        if nr_iter == 0:
            return tuple(tuple() for _ in self.include_outputs)
        # iterating inputs
        iterating_inputs = [iter_input for iter_input in self.iterating_inputs if iter_input not in moved_iterating_inputs]
        arg_values_list = [ input_dict[self._listName(iter_input)] for iter_input in iterating_inputs ] 
        # create internal node input
        common_input_dict = input_dict.copy()
        # remove list names
        for iter_input in iterating_inputs:
            common_input_dict.pop(self._listName(iter_input))
        # check nr processes
        if nr_processes is not None:
            self.nr_processes = nr_processes
        # Check if parallel processing or not
        if self.parallel_processing and self.nr_processes > 1 and nr_iter > 1:
            # queue to update tqdm process bar
            pbar_queue = Queue()
            # process to execute
            pipes, processes = tuple(zip(*[self._createProcessAndPipe(self._iterNode, self.iterating_node, pbar_queue, verbose, common_input_dict, iterating_inputs, arg_values, self.include_outputs) for arg_values in self._iterArgs(nr_iter, self.nr_processes, arg_values_list)]))
            # receive value threads (separete recv threads are needed for pipe not to hang when writing large data)
            recv_threads = [None] * len(processes)
            recv_results = [None] * len(processes)
            for i in range(len(recv_threads)):
                recv_threads[i] = Thread(target=self._pipeRecv, args=(pipes[i], recv_results, i))
                recv_threads[i].start()
            # process to update tqdm process bar
            pbar_thread = Thread(target=self._pbarUpdate, args=(pbar_queue, nr_iter, f"{self} (parallel - {self.nr_processes})", self._show_pbar, verbose))
            # start processes
            pbar_thread.start()
            # wait for recv result
            for recv_thread in recv_threads:
                recv_thread.join()
            # wait for processes
            for process in processes:
                process.join()
            # terminate pbar_thread by sending None to queue
            pbar_queue.put(None)
            # wait for pbar thread
            pbar_thread.join()
            # close queue and wait for backround thread to join
            pbar_queue.close()
            pbar_queue.join_thread()
            # check recv results
            for recv in recv_results:
                if recv is None:
                    raise RuntimeError(f"{self} did not receive result from subprocess...")
            # combine process_results
            # mapped = chain.from_iterable(process_results)
            return tuple(np.concatenate(output_list) if isinstance(output_list[0], np.ndarray) else tuple(chain.from_iterable(output_list)) for output_list in zip( *recv_results ))
        else:
            def f_single_iteration(args):
                output = self.iterating_node.run(ignore_cache=True, verbose=verbose, **common_input_dict, **{name : arg for name, arg in zip(iterating_inputs, args)})
                return output[self.include_outputs]
            # f_single_iteration = lambda args : self.iterating_node.run(ignore_cache=True, verbose=verbose, **common_input_dict, **{name : arg for name, arg in zip(self.iterating_inputs, args)}).values()
            if self._show_pbar and verbose:
                mapped = map(f_single_iteration, tqdm(zip(*arg_values_list), total = nr_iter, desc = f"{self} (sequential)"))
            else:
                mapped = map(f_single_iteration, zip(*arg_values_list))
            return  tuple(np.array(output) if is_numeric(output[0]) else output for output in zip( *mapped ))

        # return tuple( zip( *mapped ) )
        # return  tuple(np.array(output) if is_numeric(output[0]) else output for output in zip( *mapped ))
    @staticmethod
    def _pipeRecv(pipe, results, idx):
        try:
            results[idx] = pipe.recv()
        except EOFError:
            pass
        finally:
            pipe.close()

    @staticmethod
    def _pbarUpdate(pbar_queue, nr_iter, desc, show_pbar, verbose):
        if show_pbar and verbose:
            with tqdm(total = nr_iter, desc = desc) as pbar:
                for nr in iter(pbar_queue.get, None):
                    pbar.update(nr)
    
    @staticmethod
    def _createProcessAndPipe(target, *args):
        in_pipe, out_pipe = Pipe(duplex = False)
        p = Process(target = target, args = args, kwargs = {"pipe" : out_pipe})
        p.start()
        out_pipe.close()
        return in_pipe, p

    @staticmethod
    def _iterNode(iterating_node : ProcessNode, pbar_queue : Queue, verbose : bool, common_input_dict : dict, arg_names : list[str], arg_values : Iterable, include_outputs : tuple[str], pipe : Pipe) -> list:
        outputs = []
        nr_iter, iterable_list = arg_values
        update_every = int(nr_iter / 100)
        nr = 0
        for args in zip(*iterable_list):
            output = iterating_node.run(ignore_cache=True, verbose=verbose, **common_input_dict, **{name : arg for name, arg in zip(arg_names, args)})
            outputs.append(output[include_outputs])
            nr += 1
            if nr > update_every:
                pbar_queue.put(nr)
                nr = 0
        pbar_queue.put(nr)
        # join queue
        pbar_queue.close()
        pbar_queue.join_thread()
        # pivot outputs
        pivot_outputs = []
        non_numeric_outputs = []
        for i, output in enumerate(zip( *outputs )):
            if is_numeric(output[0]):
                try:
                    pivot_outputs.append(np.array(output))
                except ValueError as e:
                    raise RuntimeError(f"Unable able to concatenate numeric output: {include_outputs[i]} from node {iterating_node}.")
            else:
                pivot_outputs.append(output)
                non_numeric_outputs.append(include_outputs[i])
        # send result
        pipe.send(tuple(pivot_outputs))
        pipe.close()
        # warn about non numeric outputs
        if non_numeric_outputs:
            print(f"Output(s): {non_numeric_outputs} from node {iterating_node} is not numeric. " \
                                        + "Transfering data betwen processes can be slow.", file=sys.stderr)

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
                # enable zero length input
                if not isinstance(input_dict[self._listName(iter_input)], Iterable):
                    # add input to list
                    input_dict[self._listName(iter_input)] = [ input_dict[self._listName(iter_input)] ]
                # check length of input
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
        # handle iterating inputs with length 1
        moved_iterating_inputs = []
        for iter_input in self.iterating_inputs:
            val = input_dict[self._listName(iter_input)]
            len_ = len(val)
            if len_ != nr_iter:
                if len_ == 1:
                    # move input from iterating inputs to non iterating input
                    input_dict.pop(self._listName(iter_input))
                    input_dict[iter_input] = val[0]
                    moved_iterating_inputs.append(iter_input)
                else:
                    raise ValueError("Inconsistent lengths given for the iterating inputs, coding error...")
        return nr_iter, moved_iterating_inputs

    def _listName(self, name : str) -> str:
        return f"{self._iterating_name}Iter_{name}"

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
    
    @property
    def show_pbar(self) -> bool:
        return self._show_pbar

    @show_pbar.setter
    def show_pbar(self, val) -> None:
        self._show_pbar = bool(val)

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