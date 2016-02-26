#!/usr/bin/python

class reverseIter:
    def __init__(self,num_list):
        self.num_list = num_list
        self.i = len(self.num_list)
    def __iter__(self):
        return self
    def next(self):
        if self.i <= 0:
            raise StopIteration()
        else:
            self.i -= 1
            return self.num_list[self.i]

if __name__ == '__main__':
    iter_object = ('a','b','c') 
    aa =  reverseIter(iter_object)

    print  list(aa)
    print  list(aa)
