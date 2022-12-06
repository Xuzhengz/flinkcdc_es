package com.ocean.flinkcdc.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author 徐正洲
 * @date 2022/12/5-18:10
 * 标准地址bean
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Bzdz {

    private String title;

    private String address;

    private String xzqh;

    private String pcs;

    private Map<String, String> coordinate02;

    private String source;

    private String location_id;

    private String jdname;

    private String jwname;

    private String address_type;

    private String address_type2;

    private String wg;

    private String gd_id;

    private String gd_parent;

    private String active;
}