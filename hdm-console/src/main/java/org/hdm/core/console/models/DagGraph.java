package org.hdm.core.console.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tiantian on 8/04/16.
 */
public class DagGraph implements Serializable{

    private List nodes = new ArrayList();
    private List links = new ArrayList();


    public List getNodes() {
        return nodes;
    }

    public void setNodes(List nodes) {
        this.nodes = nodes;
    }

    public List getLinks() {
        return links;
    }

    public void setLinks(List links) {
        this.links = links;
    }

    public  DagGraph addNodes(Object node){
        nodes.add(node);
        return this;
    }

    public DagGraph addLink(Object link){
        links.add(link);
        return this;
    }
}
