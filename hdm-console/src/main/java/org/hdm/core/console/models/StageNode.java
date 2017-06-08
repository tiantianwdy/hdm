package org.hdm.core.console.models;

import java.util.List;

/**
 * Created by tiantian on 25/02/17.
 */
public class StageNode {

    String appId;
    String id;
    List<String> parents;
    String context;
    Integer parallelism;
    Boolean isLocal;
    String status;
    // for view
    String name;
    Integer group;

    public StageNode(String appId, String id, List<String> parents, String context, Integer parallelism, Boolean isLocal, String status) {
        this.appId = appId;
        this.id = id;
        this.parents = parents;
        this.context = context;
        this.parallelism = parallelism;
        this.isLocal = isLocal;
        this.status = status;
        if (isLocal) {
            if (parents == null || parents.isEmpty()) {
                this.name = "Local Stage";
                group = 4;
            } else {
                this.name = "Collaborative Stage";
                group = 2;
            }
        } else {
            this.name = "Remote Stage";
            group = 8;
        }
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getParents() {
        return parents;
    }

    public void setParents(List<String> parents) {
        this.parents = parents;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public Boolean getLocal() {
        return isLocal;
    }

    public void setLocal(Boolean local) {
        isLocal = local;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getGroup() {
        return group;
    }

    public void setGroup(Integer group) {
        this.group = group;
    }
}
