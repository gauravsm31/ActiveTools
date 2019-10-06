from itertools import combinations

class GetCollocatedLibraries(object):

    def GetLibraryPairs(self, lib_list):

        return_list = lib_list
        lib_list_sorted = sorted(lib_list)
        # get list of all subsets of length 2
        # to deal with duplicate subsets use
        # set(list(combinations(arr, r)))
        comb_list = list(combinations(lib_list_sorted, 2))

        for lib_pair in comb_list:
            return_list.append(str(lib_pair[0])+'_'+str(lib_pair[1]))

        return return_list
