package org.nicta.wdy.hdm.console.models;

/**
 * Created by tiantian on 11/04/16.
 */
public class D3Link {

    //source
    private Integer source;
    //target
    private Integer target;
    private String value;

    public D3Link(Integer source, Integer target, String value) {
        this.source = source;
        this.target = target;
        this.value = value;
    }

    public Integer getSource() {
        return source;
    }

    public void setSource(Integer source) {
        this.source = source;
    }

    public Integer getTarget() {
        return target;
    }

    public void setTarget(Integer target) {
        this.target = target;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
