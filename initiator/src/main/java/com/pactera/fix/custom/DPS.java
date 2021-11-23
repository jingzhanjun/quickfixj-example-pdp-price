package com.pactera.fix.custom;

import quickfix.StringField;

public class DPS extends StringField {
    static final long serialVersionUID = 20050617L;
    public static final int FIELD = 55;

    public DPS() {
        super(20001);
    }

    public DPS(String data) {
        super(20001, data);
    }
}