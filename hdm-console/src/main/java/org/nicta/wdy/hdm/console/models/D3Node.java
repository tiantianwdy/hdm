package org.nicta.wdy.hdm.console.models;

/**
 * Created by tiantian on 11/04/16.
 */
public class D3Node {

    private Integer id;

    private String name;

    private String type;

    private String parent;

    private String path;

    private String status;

    private Integer group;

    public D3Node(Integer id, String name, String type, String parent, String path, String status, Integer group) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.parent = parent;
        this.path = path;
        this.status = status;
        this.group = group;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getGroup() {
        return group;
    }

    public void setGroup(Integer group) {
        this.group = group;
    }
}
