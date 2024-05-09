from multiprocess import cpu_count, Process, Pipe, Queue
from tqdm import tqdm
import numpy as np
from itertools import chain
from collections.abc import Iterable
from ._ProcessNode import ProcessNode
import sys
import pickle
from threading import Thread
import os 

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
    def __init__(self, iterating_node : ProcessNode, iterating_inputs : tuple[str], iterating_name : str = "", parallel_processing : bool = False, nr_processes : int = -1, description : str = "") -> None:
        # call super init
        super().__init__(description, create_input_output_attr=False)
        # save arguments
        self._iterating_node = iterating_node
        self._iterating_name = iterating_name
        self.parallel_processing = parallel_processing
        self.nr_processes = nr_processes
        
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
        # create outputs tuple and ad _list to all names
        self._outputs = tuple(self._listName(output) for output in iterating_node.outputs)
        # create attributes
        self._createInputOutputAttributes()

    @property
    def outputs(self) -> tuple:
        return self._outputs

    def _run(self, verbose : bool, **input_dict):
        # check input_dict
        nr_iter = self._checkInput(input_dict)
        # iterating inputs
        arg_values_list = [ input_dict[self._listName(iter_input)] for iter_input in self.iterating_inputs ] 
        # create internal node input
        common_input_dict = input_dict.copy()
        # remove list names
        for iter_input in self.iterating_inputs:
            common_input_dict.pop(self._listName(iter_input))
        # create call functiona
        # f_multi_iteration = lambda arg_values : self._iterNodeMap(self.iterating_node, common_input_dict, self.iterating_inputs, arg_values)
        
        
        # Check if parallel processing or not
        if self.parallel_processing and nr_iter > 1:
            # queue to update tqdm process bar
            pbar_queue = Queue()
            # process to execute
            processes = [self._createProcessAndPipe(self._iterNode, self.iterating_node, pbar_queue, verbose, common_input_dict, self.iterating_inputs, arg_values) for arg_values in self._iterArgs(nr_iter, self.nr_processes, arg_values_list)]
            # threads
            threads = [None] * len(processes)
            results = [None] * len(processes)

            for i in range(len(threads)):
                threads[i] = Thread(target=self._pipeListener, args=(processes[i][0], results, i))
                threads[i].start()
            # process to update tqdm process bar
            pbar_proc = Thread(target=self._pbarListener, args=(pbar_queue, nr_iter, f"{self} (parallel - {self.nr_processes})", verbose))
            # start processes
            pbar_proc.start()
            # [p[1].start() for p in processes]
            # get result
            for i in range(len(threads)):
                threads[i].join()
            process_results = results #[p[0].recv() for p in processes]
            # wait for them to finnish
            [p[1].join() for p in processes]
            [p[0].close() for p in processes]
            # terminate pbar_process by sending None to queue
            pbar_queue.put(None)
            # join pbar process
            pbar_proc.join()
            # close queue and wait for backround thread to join
            pbar_queue.close()
            pbar_queue.join_thread()
            # combine process_results
            # mapped = chain.from_iterable(process_results)
            return tuple(np.concatenate(output_list) if isinstance(output_list[0], np.ndarray) else tuple(chain.from_iterable(output_list)) for output_list in zip( *process_results ))
        else:
            f_single_iteration = lambda args : self.iterating_node.run(ignore_cache=True, verbose=verbose, **common_input_dict, **{name : arg for name, arg in zip(self.iterating_inputs, args)}).values
            if verbose:
                mapped = map(f_single_iteration, tqdm(zip(*arg_values_list), total = nr_iter, desc = f"{self} (sequential)"))
            else:
                mapped = map(f_single_iteration, zip(*arg_values_list))
            return  tuple(np.array(output) if is_numeric(output[0]) else output for output in zip( *mapped ))

        # return tuple( zip( *mapped ) )
        # return  tuple(np.array(output) if is_numeric(output[0]) else output for output in zip( *mapped ))
    @staticmethod
    def _pipeListener(pipe, results, i ):
        results[i] = pipe.recv()

    @staticmethod
    def _pbarListener(pbar_queue, nr_iter, desc, verbose):
        if verbose:
            pbar = tqdm(total = nr_iter, desc = desc)
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
    def _iterNode(iterating_node : ProcessNode, pbar_queue : Queue, verbose : bool, common_input_dict : dict, arg_names : list[str], arg_values : Iterable, pipe : Pipe) -> list:
        outputs = []
        nr_iter, iterable_list = arg_values
        update_every = int(nr_iter / 100)
        nr = 0
        for args in zip(*iterable_list):
            outputs.append(iterating_node.run(ignore_cache=True, verbose=verbose, **common_input_dict, **{name : arg for name, arg in zip(arg_names, args)}).values)
            nr += 1
            if nr > update_every:
                pbar_queue.put(nr)
                nr = 0
        pbar_queue.put(nr)
        res = []
        for i, output in enumerate(zip( *outputs )):
            try:
                res.append(np.array(output))
            except:
                print("error output ", i)
                print(output)
                raise
        # res = tuple(np.array(output) if is_numeric(output[0]) else output for output in zip( *outputs ))
        # print(f"Size of payload is: {sys.getsizeof(res)/1024} KiB")
        # pkl_res = pickle.dumps(res)
        # print(f"Size of pickled payload is: {sys.getsizeof(pkl_res)/1024} KiB")
        print(os.getpid(), "sending")
        pipe.send(tuple(res))
        print(os.getpid(), "all sent")
        # pipe.send(outputs)
        pipe.close()

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
        # make sure all iterating inputs with length one
        for iter_input in self.iterating_inputs:
            val = input_dict[self._listName(iter_input)]
            len_ = len(val)
            if len_ != nr_iter:
                if len_ == 1:
                    input_dict[self._listName(iter_input)] = val * nr_iter
                else:
                    raise ValueError("Inconsistent lengths given for the iterating inputs, coding error...")
        return nr_iter

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