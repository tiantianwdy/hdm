package org.hdm.core.console.models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tiantian on 9/04/16.
 */
public class TimeLanes {

    private List<Lane> lanes = new ArrayList<Lane>();

    private List items = new ArrayList<LaneItem>();

    private Long min = 0L;

    private Long max = System.currentTimeMillis();

    public TimeLanes(List<Lane> lanes, List items) {
        this.lanes = lanes;
        this.items = items;
    }

    public TimeLanes(List<Lane> lanes, List items, Long min, Long max) {
        this.lanes = lanes;
        this.items = items;
        this.min = min;
        this.max = max;
    }

    public List<Lane> getLanes() {
        return lanes;
    }

    public void setLanes(List<Lane> lanes) {
        this.lanes = lanes;
    }

    public List getItems() {
        return items;
    }

    public void setItems(List items) {
        this.items = items;
    }

    public Long getMin() {
        return min;
    }

    public void setMin(Long min) {
        this.min = min;
    }

    public Long getMax() {
        return max;
    }

    public void setMax(Long max) {
        this.max = max;
    }
}
