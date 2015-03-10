from libpgm.nodedata import NodeData
from libpgm.graphskeleton import GraphSkeleton
from libpgm.discretebayesiannetwork import DiscreteBayesianNetwork



def main(argv):

    
    
    nd = NodeData()
    skel = GraphSkeleton()
    nd.load("../tests/unittestdict.txt")    # any input file
    skel.load("../tests/unittestdict.txt")

    # topologically order graphskeleton
    skel.toporder()

    # load bayesian network
    bn = DiscreteBayesianNetwork(skel, nd)

    # sample 
    result = bn.randomsample(10)

    # output
    print json.dumps(result, indent=2)

    #print output to target format
    output_to_ofile()

if __name__ == '__main__':
    main(sys.argv)