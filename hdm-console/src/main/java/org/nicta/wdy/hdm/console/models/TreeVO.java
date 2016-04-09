package org.nicta.wdy.hdm.console.models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tiantian on 8/04/16.
 */
public class TreeVO {

    private String name ;
    private Integer group;
    private List<TreeVO> children = null;

    public TreeVO() {
        this("", new ArrayList<TreeVO>());
    }

    public TreeVO(String name) {
        this(name, new ArrayList<TreeVO>());
    }

    public TreeVO(String name, Integer group) {
        this(name, group, new ArrayList<TreeVO>());
    }

    public TreeVO(String name, List<TreeVO> children) {
        this.name = name;
        this.children = children;
    }

    public TreeVO(String name, Integer group, List<TreeVO> children) {
        this.name = name;
        this.group = group;
        this.children = children;
    }

    public Integer getGroup() {
        return group;
    }

    public void setGroup(Integer group) {
        this.group = group;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TreeVO> getChildren() {
        return children;
    }

    public void setChildren(List<TreeVO> children) {
        this.children = children;
    }

    public TreeVO addChild(TreeVO child) {
        children.add(child);
        return this;
    }
}
