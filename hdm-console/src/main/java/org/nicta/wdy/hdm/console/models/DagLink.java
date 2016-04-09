package org.nicta.wdy.hdm.console.models;

import java.io.Serializable;

/**
 * Created by tiantian on 8/04/16.
 */
public class DagLink implements Serializable {

    //source
    private String u;
    //target
    private String v;
    private String label;

    public DagLink(String u, String v, String label) {
        this.u = u;
        this.v = v;
        this.label = label;
    }

    public String getU() {
        return u;
    }

    public void setU(String u) {
        this.u = u;
    }

    public String getV() {
        return v;
    }

    public void setV(String v) {
        this.v = v;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
