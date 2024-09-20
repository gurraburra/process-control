from collections.abc import Iterable
import copy

class NodeInputOutput(object):
    def __init__(self, owner : object, name : str) -> None:
        self.__owner = owner
        self.__name = None
        self.name = name
        
    @property
    def owner(self) -> object:
        return self.__owner
    @owner.setter
    def owner(self, new_owner : object):
        # assert no previous owner
        assert self.__owner is None, f"You are not allowed to change the owner of a previous defined input output."
        self.__owner = new_owner
    @property
    def name(self) -> object:
        return self.__name
    @name.setter
    def name(self, name) -> None:
        assert self.__name is None, f"Cannot change name of input output that has already been set."
        if name is not None:
            assert isinstance(name, str), f"Inputs and outputs must be strings but a {type(name)} was given: {name}."
            assert name.isidentifier(), f"Inputs and outputs must be valid python identifiers: {name}."
            self.__name = name
        
    def __hash__(self) -> int:
        return hash((self.owner, self.name))
        
    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, type(self)):
            return self.owner == other.owner and self.name == other.name
        return False
        
    def __str__(self):
        return f"{self.owner}: {self.name}"
    def __repr__(self):
        return self.__str__()
    
    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, copy.deepcopy(v, memo))
        return result
    
    def __getstate__(self):
        return self.__dict__
    def __setstate__(self, d):
        self.__dict__ = d

class TupleSubtract(tuple):
    def __new__(cls, iterable: Iterable = ...):
        return super().__new__(cls, iterable)
    
    def __sub__(self, obj : tuple):
        if not isinstance(obj, Iterable) or isinstance(obj, NodeInputOutput):
            obj = (obj, )
        return TupleSubtract(tuple(item for item in self if not self.__special_contains__(item, obj)))
        
    def __truediv__(self, obj : tuple):
        return self.__sub__(obj)

    def __floordiv__(self, obj : tuple):
        return self.__sub__(obj)
    
    def __div__(self, obj : tuple):
        return self.__sub__(obj)
    
    # need this special contain since the tuple might contain NodeOutput which overrides the __eq__ and returns a _BinaryOperand
    def __special_contains__(self, item: object, list_ : list) -> bool:
        if isinstance(item, NodeInputOutput):
            return any(NodeInputOutput.__eq__(item, list_item) for list_item in list_)
        else:
            return item in list_
    
class NodeDict(object):
    def __init__(self, owner : object, keys : Iterable, iterable : Iterable) -> None:
        super().__init__()
        self.__owner = owner
        self.__keys = TupleSubtract(keys)
        self.__tuple = TupleSubtract(iterable)

    @property
    def _owner(self) -> object:
        return self.__owner
    
    #@property
    def _keys(self) -> Iterable:
        return self.__keys
    
    #@property
    def _values(self) -> Iterable:
        return self.__tuple
        
    def __len__(self):
        return len(self.__tuple)
    
    def __getitem__(self, key):
        return self.__getattr__(key)

    def __getattr__(self, key):
        if isinstance(key, int):
            try:
                return self.__tuple[key]
            except IndexError:
                raise AttributeError(f"key: {key} not valid integer")
        elif isinstance(key, (tuple,list)):
            return TupleSubtract(NodeDict.__getattr__(self, k) for k in key)
        elif key in self.__keys:
            return self.__tuple[self.__keys.index(key)]
        elif key == 'all':
            return self.__tuple
        else:
            raise AttributeError(f"key: '{key}' not valid string")            
        
    def __iter__(self):
        for key in self._keys():
            yield key
        
    def __str__(self) -> str:
        return f"{self._keys()} -> {self._values()}"
    
    def __repr__(self) -> str:
        return self.__str__()

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, copy.deepcopy(v, memo))
        return result

    def __getstate__(self):
        return self.__dict__
    def __setstate__(self, d):
        self.__dict__ = d
        
        
class NodeMapping(NodeDict):
    def __init__(self,  owner: object, keys: Iterable, iterable: Iterable, is_input : bool) -> None:
        super().__init__(owner, keys, iterable)
        self.__input_output_str = "input" if is_input else "output"

    @property
    def _input_output_str(self):
        return self.__input_output_str

    def __getattr__(self, key):
        try:
            return super().__getattr__(key)
        except Exception as e:
            errmsg = f"could not find {object.__getattribute__(self, '_NodeMapping__input_output_str')} in node {object.__getattribute__(self, '_NodeDict__owner')}"
            e.add_note(errmsg)
            raise
            # for some reason parallel processing require direct mapping to properties are required
            # raise ValueError(f"could not find {object.__getattribute__(self, '_NodeMapping__input_output_str')} in node {object.__getattribute__(self, '_NodeDict__owner')}") from e

    def __str__(self) -> str:
        return f"{self._owner}: {self._input_output_str} -> {self._keys()}"

class NodeRunOutput(NodeDict):
    def __getattr__(self, key):
        try:
            return super().__getattr__(key)
        except:
            # for some reason parallel processing require direct mapping to properties are required
            raise ValueError(f"{object.__getattribute__(self, '_NodeDict__owner')} does not have an output named '{key}'.")
        
    # def __str__(self) -> str:
    #     return f"{self._owner}: Produced output {super().__str__()}"
    
    def __repr__(self) -> str:
        outputs = []
        for output, value in zip(self._keys(), self._values()):
            outputs.append(f"{output} -> {value}")
        return f"{self._owner}: Computed output\n" + "\n".join(outputs)
    
    def keys(self) -> Iterable:
        return self._keys()
    
    def values(self) -> Iterable:
        return self._values()

class NodeRunInput(NodeDict):
    def __getattr__(self, key):
        try:
            return super().__getattr__(key)
        except:
            # for some reason parallel processing require direct mapping to properties are required
            raise ValueError(f"{object.__getattribute__(self, '_NodeDict__owner')} does not have an input named '{key}'.")
        
    # def __str__(self) -> str:
    #     return f"{self._owner}: Produced output {super().__str__()}"
    
    def __repr__(self) -> str:
        inputs = []
        for input, value in zip(self._keys(), self._values()):
            inputs.append(f"{input} -> {value}")
        return f"{self._owner}: Given input\n" + "\n".join(inputs)
    
    def keys(self) -> Iterable:
        return self._keys()
    
    def values(self) -> Iterable:
        return self._values()