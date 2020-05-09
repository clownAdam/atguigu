package com.atguigu.project.microblog;

import java.io.IOException;

/**
 * @author clown
 */
public class WeiBo {
    public static void init() throws IOException {
        //创建相关命名空间,表
        WeiBoUtil.createNamespace(Constant.NAMESPACE);
        WeiBoUtil.createTable(Constant.CONTENT,1, "info");
        WeiBoUtil.createTable(Constant.RELATIONS,1, "attends","fans");
        WeiBoUtil.createTable(Constant.INBOX,2, "info");
    }

    public static void main(String[] args) throws IOException {
        //测试
//        init();
        /*1001,1002发布微博*/
//        WeiBoUtil.createData("1001","today weather good");
//        WeiBoUtil.createData("1002","today weather bad");
        /*1001关注1002和1003*/
//        WeiBoUtil.addAttend("1001","1002","1003");
        /*获取1001初始化页面信息*/
        WeiBoUtil.getInit("1002");
        /*1003发布微博*/
        /*获取1001初始化页面信息*/
    }
}
