package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Amardeep on 4/14/17.
 */

public class QueryResults implements Serializable {

    public int Next_node;
    public int Destination;
    public int Last_node;
    public HashMap<String,String> Map;
    public String Query_type;

    public QueryResults(HashMap Map, int Last_node, int Destination, int Next_node, String Query_type) {
        this.Map = Map;
        this.Last_node = Last_node;
        this.Next_node = Next_node;
        this.Destination = Destination;
        this.Query_type = Query_type;
    }
}
