package org.hdm.core.console.models;

/**
 * Created by tiantian on 9/04/16.
 */
public class Lane {
    private Integer id;
    private String label;

    public Lane(Integer id, String label) {
        this.id = id;
        this.label = label;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
