
# mysqlMP-client  
  
mysql高可用管理
  
  
# 功能  
  
 1. mysql状态检查
 2. 主从切换执行
 3. master宕机复检
 4. 宕机差异binlog获取解析并追加、回滚
 5. 自动恢复宕机节点关系
 6. 审计日志（开发中）
 7. 慢sql分析（开发中）
  
  
## 使用方法：  
下载源码进行编译，或者下载最新release下的可执行文件。配置参数(--help):
 1. binlogdir:  binlog文件保存目录
 2. host：本地mysql地址及端口（127.0.0.1:3306）
 3. user:	连接本地mysql的用户名
 4. password： 密码
 5. repluser： 主从同步所使用的用户名
 6. replpasswd： 主从同步密码
 
需放于mysql节点上运行，在宕机复检时使用的repluser进行登陆连接，所以该账户需要对应权限

    
