package org.nicta.wdy.hdm.console.models;

/**
 * Created by tiantian on 9/04/16.
 */
public class LaneItem {

    private String id;

    private Integer lane;

    private Long start;

    private Long end;

    private String cls;

    private String desc;

    public LaneItem(String id, Integer laneId, Long start, Long end, String cls, String desc) {
        this.id = id;
        this.lane = laneId;
        this.start = start;
        this.end = end;
        this.cls = cls;
        this.desc = desc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getLane() {
        return lane;
    }

    public void setLane(Integer lane) {
        this.lane = lane;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public String getCls() {
        return cls;
    }

    public void setCls(String cls) {
        this.cls = cls;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
