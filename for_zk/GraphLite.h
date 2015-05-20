/**
 * @file GraphLite.h
 * @author  Songjie Niu <niusongjie@ict.ac.cn>, Shimin Chen <chensm@ict.ac.cn>
 * @version 0.1
 *
 * @section LICENSE 
 * 
 * TBD
 *
 * 
 * @section DESCRIPTION
 * 
 *  This file defines the graphlite API.  It should be included by vertex programs.
 *
 */

#ifndef GRAPHLITE_H
#define GRAPHLITE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <string>
#include <vector>
#include <fstream>

#include "hdfs.h"

/* ------------------------------------------------------------ */
#define MAXINPUTLEN 100

/** Definition of InputFormatter class. */
// 
// GraphLite engine will call an InputFormatter as follows:
//
//  open();
//  getVertexNum(); getEdgeNum();
//  getVertexValueSize(); getEdgeValueSize(); getMessageValueSize();
//  loadGraph();
//  close();
// 
class InputFormatter {
public:
    hdfsFile m_hdfs_file;       /**< input file handle, for hdfs */
    std::ifstream m_local_file; /**< input file stream, for local file system */
    const char* m_ptotal_vertex_line; /**< pointer of total vertex count line */
    const char* m_ptotal_edge_line;   /**< pointer of total edge count line */
    int64_t m_total_vertex;     /**< total vertex count of local subgraph */
    int64_t m_total_edge;       /**< total edge count of local subgraph */
    int m_n_value_size;         /**< vertex value type size */
    int m_e_value_size;         /**< edge value type size */
    int m_m_value_size;         /**< message value type size */
    int m_vertex_num_size;      /**< vertex number size */
    int m_edge_num_size;        /**< edge number size */
    tOffset m_next_edge_offset; /**< offset for next edge */
    std::string m_total_vertex_line; /**< buffer of local total vertex count line */
    std::string m_total_edge_line;   /**< buffer of local total edge count line */
    char m_buf_line[MAXINPUTLEN];    /**< buffer of hdfs file line */
    std::string m_buf_string;

public:
    /**
     * Open input file, virtual method.
     * @param pin_path input file path
     */
    virtual void open(const char* pin_path);

    /** Close input file, virtual method. */
    virtual void close();

    /**
     * Get vertex number, pure virtual method.
     * @return total vertex number in local subgraph
     */
    virtual int64_t getVertexNum() = 0;

    /** Get vertex number line */
    void getVertexNumLine();

    /**
     * Get edge number, pure virtual method.
     * @return total edge number in local subgraph
     */
    virtual int64_t getEdgeNum() = 0;

    /** Get edge number line */
    void getEdgeNumLine();

    /**
     * Get vertex value type size, pure virtual method.
     * @return vertex value type size
     */
    virtual int getVertexValueSize() = 0;

    /**
     * Get edge value type size, pure virtual method.
     * @return edge value type size
     */
    virtual int getEdgeValueSize() = 0;

    /**
     * Get message value type size, pure virtual method.
     * @return message value type size
     */
    virtual int getMessageValueSize() = 0;

    /**
     * Get edge line, for user.
     * Read from current file offset.
     * @return a string of edge in local subgraph
     */
    const char* getEdgeLine();

    /**
     * Add one vertex to Node array.
     * @param vid vertex id
     * @param pvalue pointer of vertex value
     * @param outdegree vertex outdegree
     */
    void addVertex(int64_t vid, void* pvalue, int outdegree);

    /**
     * Add one edge to Edge array.
     * @param from edge source vertex id
     * @param to edge destination vertex id
     * @param pweight pointer of edge weight
     */
    void addEdge(int64_t from, int64_t to, void* pweight);

    /** Load local subgraph, pure virtual method. */
    virtual void loadGraph() = 0;

    /** Destructor. */
    ~InputFormatter();
}; // definition of InputFormatter class


/* ------------------------------------------------------------ */
/** Definition of OutputFormatter class. */
class OutputFormatter {
public:
    /** Definition of ResultIterator class. */
    class ResultIterator {
    public:
        /**
         * Get the result after computation.
         * @param vid reference of vertex id to be got
         * @param pvalue pointer of vertex value
         */
        void getIdValue(int64_t& vid, void* pvalue);

        /** Go to visit next element. */
        void next();

        /**
         * Judge if iterator terminates or not.
         * @retval true done
         * @retval false not
         */
        bool done();
    };

public:
    hdfsFile m_hdfs_file;       /**< output file handle, for hdfs */
    std::ofstream m_local_file; /**< output file stream, for local file system */

public:
    /**
     * Open output file, virtual method.
     * @param pout_path output file path
     */
    virtual void open(const char* pout_path);

    /** Close output file, virtual method. */
    virtual void close();

    /**
     * Write next result line, for user.
     * Write to current file offset.
     * @param pbuffer buffer of result line in string
     * @param len length of result line string
     */
    void writeNextResLine(char* pbuffer, int len);

    /** Write local subgraph computation result, pure virtual method. */
    virtual void writeResult() = 0;

    /** Destructor, virtual method. */
    virtual ~OutputFormatter() {}
}; // definition of OutputFormatter class


/* ------------------------------------------------------------ */
/** Definition of AggregatorBase class. */
class AggregatorBase {
public:
    /** Initialize, mainly for aggregator value. */
    virtual void init() = 0;

    /**
     * Get aggregator value type size.
     * @return aggregator value type size.
     */
    virtual int getSize() const = 0;

    /**
     * Get aggregator global value.
     * @return pointer of aggregator global value
     */
    virtual void* getGlobal() = 0;

    /**
     * Set aggregator global value.
     * @param p pointer of value to set global as
     */
    virtual void setGlobal(const void* p) = 0;

    /**
     * Get aggregator local value.
     * @return pointer of aggregator local value
     */
    virtual void* getLocal() = 0;

    /**
     * Merge method for global.
     * @param p pointer of value to be merged
     */
    virtual void merge(const void* p) = 0;

    /**
     * Accumulate method for local.
     * @param p pointer of value to be accumulated
     */
    virtual void accumulate(const void* p) = 0;
}; // definition of AggregatorBase class


/* ------------------------------------------------------------ */
/** Definition of Aggregator class. */
template<typename AggrValue>
class Aggregator: public AggregatorBase {
public:
    AggrValue m_global; /**< aggregator global value of AggrValue type */
    AggrValue m_local;  /**< aggregator local value of AggrValue type */

public:
    virtual void init() = 0;
    virtual int getSize() const {
        return sizeof(AggrValue);
    }
    virtual void* getGlobal() = 0;
    virtual void setGlobal(const void* p) = 0;
    virtual void* getLocal() = 0;
    virtual void merge(const void* p) = 0;
    virtual void accumulate(const void* p) = 0;
}; // definition of Aggregator class


/* ------------------------------------------------------------ */
/** Definition of GenericArrayIterator class. */
class GenericArrayIterator {
public:
    char* m_pbegin;       /**< pointer of iterator begin position */
    char* m_pend;         /**< pointer of iterator end position */
    int   m_element_size; /**< size of array element */

public:
    /**
     * Constructor.
     * @param pbegin iterator begin position
     * @param pend iterator end position
     * @param size array element size
     */
    GenericArrayIterator(char* pbegin, char* pend, int size):
        m_pbegin(pbegin), m_pend(pend), m_element_size(size) {}

    /**
     * Get iterator size.
     * @return count of elements to visit
     */
    int64_t size() { return (int64_t)(m_pend - m_pbegin) / m_element_size; }

    /**
     * Get current element position.
     * @return pointer of current element
     */
    char* current() { return m_pbegin; }

    /** Go to visit next element. */
    void next() { m_pbegin += m_element_size; }

    /**
     * Judge if iterator terminates or not.
     * @retval true done
     * @retval false not
     */
    bool done() { return (m_pbegin >= m_pend); }
}; // definition of GenericArrayIterator class


/* ------------------------------------------------------------ */
/** Definition of node-node Message struct. */
typedef struct Msg {
    int64_t s_id;    /**< source vertex id of the message */
    int64_t d_id;    /**< destination vertex id of the message */
    char message[0]; /**< start positon of memory to store message content */

    static int m_value_size; /**< messge value size in character */
    static int m_size;       /**< total size of a piece of message, sizeof(Msg) + m_value_size */
} Msg;

/** Definition of GenericLinkIterator class. */
class GenericLinkIterator {
public:
    std::vector<Msg*>*  m_pvector;     /**< pointer of vector to be iterated on */
    int            m_vector_size; /**< vector size */
    int            m_cur_index;   /**< index of current element in vector */

public:
    /**
     * Constructor.
     * @param pvector pointer of vector to be iterated on
     */
    GenericLinkIterator(std::vector<Msg*>* pvector): m_pvector(pvector) {
        m_vector_size = pvector->size();
        m_cur_index = 0;
    }

    /**
     * Get current element.
     * @return current element of vector
     */
    char* getCurrent() { return (char *)(*m_pvector)[m_cur_index]; }

    /** Go to visit next element. */
    void next() { ++m_cur_index; }

    /**
     * Judge if iterator terminates or not.
     * @retval true done
     * @retval false not
     */
    bool done() { return m_cur_index == m_vector_size; }
}; // definition of GenericLinkIterator class


/* ------------------------------------------------------------ */
/** Definition of Edge struct. */
typedef struct Edge {
    int64_t from;   /**< start vertex id of the edge */
    int64_t to;     /**< end vertex id of the edge */
    char weight[0]; /**< start positon of memory to store edge weight */

    static int e_value_size; /**< edge value size in character */
    static int e_size;       /**< total size of an edge, sizeof(Edge) + e_value_size */
} Edge;

/** Definition of Node class. */
class Node {
public:
    bool m_active;        /**< vertex state: active m_active=1, inactive m_active=0 */
    int64_t m_v_id;       /**< vertex id */
    int m_out_degree;     /**< vertex outdegree */
    int64_t m_edge_index; /**< index of first edge from this vertex in edge array */
    std::vector<Msg*> m_cur_in_msg; /**< current superstep in-message pointer list */
    char value[0];                  /**< start positon of memory to store node value */

public:
    static int n_value_size; /**< node value size in character */
    static int n_size;	     /**< total size of a node, sizeof(Node) + n_value_size */

public:
    /**
     * Get a Node structure of index(begin at 0) in node array.
     * @param index node index
     * @return a Node structure of index in node array
     */
    static Node& getNode(int index);

    /**
     * Get an Eode structure of index in edge array.
     * @param index edge index
     * @return an Edge structure of index in edge array
     */
    static Edge& getEdge(int index);

    /** Initialize pointers of in-message lists. */
    void initInMsg();

    /**
     * Receive a new piece of message for next superstep.
     * Link the message to next_in_msg list.
     * @param pmsg pointer of the message
     */
    void recvNewMsg(Msg* pmsg);

    /** Free current in-message list to freelist. */
    void clearCurInMsg();

    /** Free memory of in-message lists vector allocation. */
    void freeInMsgVector();

    /**
     * Get current superstep number.
     * @return current superstep number
     */
    int getSuperstep() const;

    /**
     * Get vertex id.
     * @return vertex id
     */
    int64_t getVertexId() const;

    /**
     * Vote to halt.
     * Change vertex state to be inactive.
     */
    void voteToHalt();

    /**
     * Get a generic link iterator.
     * @return a pointer of GenericLinkIterator
     */
    GenericLinkIterator* getGenericLinkIterator();

    /**
     * Get a generic array iterator.
     * @return a pointer of GenericArrayIterator
     */
    // GenericArrayIterator* getGenericArrayIterator();

    /**
     * Send a piece of node-node message to target vertex.
     * @param dest_vertex destination vertex id
     * @param pmessage pointer of the message to be sent
     */
    void sendMessageTo(int64_t dest_vertex, const char* pmessage);

    /**
     * Send a piece of node-node message to all outedge-target vertex.
     * Call sendMessageTo() for all outedges.
     * @param pmessage pointer of the message to be sent
     * @see sendMessageTo()
     */
    void sendMessageToAllNeighbors(const char* pmessage);

    /**
     * Get global value of some aggregator.
     * @param aggr index of aggregator, count from 0
     */
    const void* getAggrGlobal(int aggr);

    /**
     * Accumulate local value of some aggregator.
     * @param aggr index of aggregator, count from 0
     */
    void accumulateAggr(int aggr, const void* p);
}; // definition of Node class


/* ------------------------------------------------------------ */
/** Definition of VertexBase class. */
class VertexBase {
public:
    Node* m_pme; /**< pointer of Node class */

public:
    /**
     * Set pointer to Node class.
     * @param p corresponding Node structure position
     */
    void setMe(Node* p) { m_pme = p; }

    /**
     * Get vertex value type size, pure virtual method.
     * @return vertex value type size
     */
    virtual int getVSize() const = 0;

    /**
     * Get edge value type size, pure virtual method.
     * @return edge value type size
     */
    virtual int getESize() const = 0;

    /**
     * Get message value type size, pure virtual method.
     * @return message value type size
     */
    virtual int getMSize() const = 0;

    /**
     * Get vertex id.
     * @return vertex id
     */
    virtual int64_t getVertexId() const = 0;

    /**
     * Get current superstep number, pure virtual method.
     * @return current superstep number
     */
    virtual int getSuperstep() const = 0;

    /**
     * Vote to halt, pure virtual method.
     * Change vertex state to be inactive.
     */
    virtual void voteToHalt() = 0;

    /**
     * Compute at active vertex, pure virtual method.
     * @param pmsgs generic pointer of received message iterator
     */
    virtual void compute(GenericLinkIterator* pmsgs) = 0;
}; // definition of VertexBase class


/* ------------------------------------------------------------ */
/** Definition of Vertex class. */
template<typename VertexValue, typename EdgeValue, typename MessageValue>
class Vertex: public VertexBase {
public:

    /** Definition of MessageIterator class. */
    class MessageIterator: public GenericLinkIterator {
    public:
        /**
         * Get message value.
         * @return message value
         */
        const MessageValue& getValue() {
            return *( (MessageValue *)( getCurrent() + offsetof(Msg, message) ) );
        }
    };

    /** Definition of OutEdgeIterator class. */
    class OutEdgeIterator: public GenericArrayIterator {
    public:
        /**
         * Constructor with arguments.
         * @param pbegin iterator begin position
         * @param pend iterator end position
         * @param size array element size
         */
        OutEdgeIterator(char* pbegin, char* pend, int size):
            GenericArrayIterator(pbegin, pend, size) {}

        /**
         * Get current edge target vertex id.
         * @see current()
         * @return target vertex id
         */
        int64_t target() {
            char* p = current();
            return ( (Edge *)p )->to;
        }
        /**
         * Get edge value.
         * @see current()
         * @return edge value
         */
        const EdgeValue& getValue() {
            char* p = current();
            return *( (EdgeValue *)( (Edge *)p )->weight );
        }
    };

public:
    /**
     * Compute at active vertex, pure virtual method.
     * @param pmsgs specialized pointer of received message iterator
     */
    virtual void compute(MessageIterator* pmsgs) = 0; // Virtual is necessary.

    /**
     * Compute at active vertex.
     * @see compute(MessageIterator*)
     * @param pmsgs generic pointer of received message iterator
     */
    void compute(GenericLinkIterator* pmsgs) {
        compute( (MessageIterator *)pmsgs ); // cast to compute() below
    }

    /**
     * Get vertex value type size.
     * @return vertex value type size
     */
    int getVSize() const {
        return sizeof(VertexValue);
    }

    /**
     * Get edge value type size.
     * @return edge value type size
     */
    int getESize() const {
        return sizeof(EdgeValue);
    }

    /**
     * Get message value type size.
     * @return message value type size
     */
    int getMSize() const {
        return sizeof(MessageValue);
    }

    /**
     * Get current superstep number.
     * @see Node::getSuperstep()
     * @return current superstep number
     */
    int getSuperstep() const {
        return m_pme->getSuperstep();
    }

    /**
     * Get vertex id.
     * @see Node::getVertexId()
     * @return vertex id
     */
    int64_t getVertexId() const {
        return m_pme->getVertexId();
    }

    /**
     * Vote to halt.
     * @see Node::voteToHalt()
     * Change vertex state to be inactive.
     */
    void voteToHalt() {
        return m_pme->voteToHalt();
    }

    /**
     * Get vertex value.
     * @see Node::value
     * @return vertex value
     */
    const VertexValue& getValue() {
        return *( (VertexValue *)m_pme->value );
    } // Type cast has lower priority than "->" operator.

    /**
     * Mutate vertex value.
     * @see Node::value
     * @return vertex value position
     */
    VertexValue* mutableValue() {
        return (VertexValue *)m_pme->value;
    } // Type cast has lower priority than "->" operator.

    /**
     * Get an out-edge iterator.
     * @see OutEdgeIterator::OutEdgeIterator()
     * @return an object of OutEdgeIterator class
     */
    OutEdgeIterator getOutEdgeIterator() {
        OutEdgeIterator out_edge_iterator(
        (char *)&(m_pme->getEdge(m_pme->m_edge_index)),
        (char *)&(m_pme->getEdge(m_pme->m_edge_index + m_pme->m_out_degree)),
        Edge::e_size );
        return out_edge_iterator;
    }

    /**
     * Send a piece of node-node message to target vertex.
     * @param dest_vertex destination vertex id
     * @param message content of the message to be sent
     * @see Node::sendMessageTo()
     */
    void sendMessageTo(int64_t dest_vertex, const MessageValue& message) {
        m_pme->sendMessageTo(dest_vertex, (const char *)&message);
    }

    /**
     * Send a piece of node-node message to all outedge-target vertex.
     * @param message content of the message to be sent
     * @see Node::sendMessageToAllNeighbors()
     */
    void sendMessageToAllNeighbors(const MessageValue& message) {
        m_pme->sendMessageToAllNeighbors( (const char *)&message );
    }

    /**
     * Get global value of some aggregator.
     * @param aggr index of aggregator, count from 0
     * @see Node::getAggrGlobal()
     */
    const void* getAggrGlobal(int aggr) {
        return m_pme->getAggrGlobal(aggr);
    }

    /**
     * Accumulate local value of some aggregator.
     * @param aggr index of aggregator, count from 0
     * @see Node::accumulateAggr()
     */
    void accumulateAggr(int aggr, const void* p) {
        m_pme->accumulateAggr(aggr, p);
    }

    /** Destructor, virtual method. */
    virtual ~Vertex() {}
}; // definition of Vertex class


/* ------------------------------------------------------------ */
#define NAME_LEN 128       // machine hostname length

/** Machine address structure. */
typedef struct Addr {
    int id;                  /**< machine id: master 0, worker from 1 */
    char hostname[NAME_LEN]; /**< machine hostname */
    int port;                /**< machine port for process */
} Addr;


/** Definition of Graph class. */
class Graph {
public:
    int m_machine_cnt;       /**< machine count, one master and some workers */
    Addr* m_paddr_table;     /**< address table, master 0 workers from 1 */
    int m_hdfs_flag;         /**< read input from hdfs or local-fs, hdfs 1 local-fs 0 */
    const char* m_pfs_host;  /**< hdfs host */
    int m_fs_port;           /**< hdfs port */
    const char* m_pin_path;  /**< input file path */
    const char* m_pout_path; /**< output file path */
    InputFormatter* m_pin_formatter;   /**< pointer of InputFormatter */
    OutputFormatter* m_pout_formatter; /**< pointer of OutputFormatter */
    int m_aggregator_cnt;              /**< aggregator count */
    AggregatorBase** m_paggregator;    /**< pointers of AggregatorBase */
    VertexBase* m_pver_base;           /**< pointer of VertexBase */

public:
    void setNumHosts(int num_hosts) {
        if (num_hosts <= 0) return;

        m_machine_cnt = num_hosts;
        m_paddr_table = new Addr[num_hosts];
    }

    void setHost(int id, const char *hostname, int port) {
        if (id<0 || id>=m_machine_cnt) return;

        m_paddr_table[id].id = id;
        strncpy(m_paddr_table[id].hostname, hostname, NAME_LEN-1);
        m_paddr_table[id].hostname[NAME_LEN-1]= '\0';
        m_paddr_table[id].port = port;
    }

    void regNumAggr(int num) {
        if (num <= 0) return;

        m_aggregator_cnt= num;
        m_paggregator= new AggregatorBase*[num];
    }

    void regAggr(int id, AggregatorBase *aggr) {
        if (id<0 || id>=m_aggregator_cnt) return;

        m_paggregator[id]= aggr;
    }


public:
    Graph(){
      setNumHosts(1);
      setHost(0, "localhost", 1411);

      m_hdfs_flag= 0; // use local file by default

      m_aggregator_cnt= 0;
      m_paggregator= NULL;
    }

    /**
     * Initialize, virtual method. All arguments from command line.
     * @param argc algorithm argument number 
     * @param argv algorithm arguments
     */
    virtual void init(int argc, char* agrv[]) {}

    /**
     * Master computes per superstep to judge if supersteps terminate,
     * virtual method.
     * @param superstep current superstep number
     * @param paggr_base aggregator base pointer
     * @retval 1 supersteps terminate
     * @retval 0 supersteps not terminate
     */
    virtual int masterComputePerstep(int superstep, AggregatorBase** paggr_base) {
        return 0;
    }

    /** Terminate, virtual method. */
    virtual void term() {}

    /** Destructor, virtual method. */
    virtual ~Graph() {
      if(m_paggregator) delete[] m_paggregator;
      if(m_paddr_table) delete[] m_paddr_table;
    }
}; // definition of Graph class

/**
 * A type definition for function pointer.
 * Function has no arguments and return value of Graph* type.
 */
typedef Graph* (* GraphCreateFn)();

/**
 * A type definition for function pointer.
 * Function has one argument of Graph* type and no return value.
 */
typedef void (* GraphDestroyFn)(Graph*);


/* ------------------------------------------------------------ */

#endif /* GRAPHLITE_H */
