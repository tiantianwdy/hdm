package org.hdm.core.console.models;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by tiantian on 8/04/16.
 */
public class HDMNode implements Serializable{

    private  String id;

    private String name;

    private String version;

    private String func;

    private String type;

    private String location;

    private String dependency;

    private String parallelism;

    private String partitioner;

    private String[] input;

    private String[] output;

    private Boolean isCache;

    private Long startTime;

    private Long endTime;

    private String status;

    private Integer group;


    public HDMNode(String id, String name, String version, String func, String type, String location, String dependency, String parallelism, String partitioner, String[] input, String[] output, Boolean isCache, Long startTime, Long endTime, String status, Integer group) {
        this.id = id;
        this.name = name;
        this.version = version;
        this.func = func;
        this.type = type;
        this.location = location;
        this.dependency = dependency;
        this.parallelism = parallelism;
        this.partitioner = partitioner;
        this.input = input;
        this.output = output;
        this.isCache = isCache;
        this.startTime = startTime;
        this.endTime = endTime;
        this.status = status;
        this.group = group;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getFunc() {
        return func;
    }

    public void setFunc(String func) {
        this.func = func;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
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

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String[] getInput() {
        return input;
    }

    public void setInput(String[] input) {
        this.input = input;
    }

    public String[] getOutput() {
        return output;
    }

    public void setOutput(String[] output) {
        this.output = output;
    }

    public Boolean getIsCache() {
        return isCache;
    }

    public void setIsCache(Boolean isCache) {
        this.isCache = isCache;
    }

    public String getDependency() {
        return dependency;
    }

    public void setDependency(String dependency) {
        this.dependency = dependency;
    }

    public String getParallelism() {
        return parallelism;
    }

    public void setParallelism(String parallelism) {
        this.parallelism = parallelism;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(String partitioner) {
        this.partitioner = partitioner;
    }
}
