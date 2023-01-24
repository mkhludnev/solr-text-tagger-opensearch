package org.opensearch.solrtexttagger.impl;

import org.opensearch.common.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Tag {
    private int startOffset;
    private int endOffset;
    private String matchText;
    private List<String> ids;

    public Tag() {
    }

    public Tag(int startOffset, int endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(int startOffset) {
        this.startOffset = startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(int endOffset) {
        this.endOffset = endOffset;
    }

    public String getMatchText() {
        return matchText;
    }

    public void setMatchText(String matchText) {
        this.matchText = matchText;
    }

    public List<String> getIds() {
        return ids == null ? Collections.emptyList() : ids;
    }

    public void addId(String id) {
        if (this.ids==null) {
            this.ids = new ArrayList<>();
        }
        this.ids.add(id);
    }

    public void setIds(List<String> strings) {
        this.ids = strings;
    }
}
