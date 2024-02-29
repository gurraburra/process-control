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

class NodeDict(object):
    def __init__(self, owner : object, keys : Iterable, iterable : Iterable) -> None:
        super().__init__()
        self.__owner = owner
        self.__keys = keys
        self.__tuple = tuple(iterable)

    @property
    def _owner(self) -> object:
        return self.__owner
    
    @property
    def keys(self) -> Iterable:
        return self.__keys
    
    @property
    def values(self) -> Iterable:
        return self.__tuple
        
    def __len__(self):
        return len(self.__tuple)
    
    def __getitem__(self, key):
        return self.__getattr__(key)

    def __getattr__(self, key):
        if isinstance(key, int):
            return self.__tuple[key]
        elif isinstance(key, (tuple,list)):
            return tuple(self.__getattr__(k) for k in key)
        elif key in self.__keys:
            return self.__tuple[self.__keys.index(key)]
        elif key == 'all':
            return self.__tuple
        else:
            raise AttributeError
            # return object.__getattribute__(self, key)
        
    def __iter__(self):
        for data in self.values:
            yield data
        
    def __str__(self) -> str:
        return f"{self.keys} -> {self.values}"
    
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
        except:
            # for some reason parallel processing require direct mapping to properties are required
            raise ValueError(f"{object.__getattribute__(self, '_NodeDict__owner')} does not have an {object.__getattribute__(self, '_NodeMapping__input_output_str')} named '{key}'.")

    def __str__(self) -> str:
        return f"{self._owner}: {self._input_output_str} -> {self.keys}"

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
        for output, value in zip(self.keys, self.values):
            outputs.append(f"{output} -> {value}")
        return f"{self._owner}: Computed output\n" + "\n".join(outputs)
