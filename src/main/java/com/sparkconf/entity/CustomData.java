package com.sparkconf.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CustomData implements Serializable {
    private String udid;
    private List<String> brands;
    private List<String> categorys;
}
