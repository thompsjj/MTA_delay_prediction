from collections import MutableSequence

class uniquelist(MutableSequence):
    """This is a list type that only adds unique values. Otherwise it behaves
    entirely normally"""
    def __init__(self, data=None):
        super(uniquelist, self).__init__()
        if not (data is None):
            self._list = list(data)
        else:
            self._list = list()
    def __len__(self):
        return len(self._list)
    def __getitem__(self, ii):
        return self._list[ii]
    def __delitem__(self, ii):
        del self._list[ii]
    def __setitem__(self, ii, val):
        self._list[ii] = val
        return self._list[ii]
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return """<uniquelist %s>""" % self._list
    def insert(self, ii, val):
        self._list.insert(ii, val)
    def append(self, val):
        if val not in self._list:
            list_idx = len(self._list)
            self.insert(list_idx, val)
