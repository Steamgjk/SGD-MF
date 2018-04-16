//
//  main.cpp
//  linux_socket_api
//
//  Created by bikang on 16/11/2.
//  Copyright (c) 2016年 bikang. All rights reserved.
//

#include <iostream>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <time.h>
#include <vector>
#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>

using namespace std;

#define BUFFER_SIZE 1024

#define WORKER_NUM 4
char* remote_ips[WORKER_NUM] = {"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"};
int remote_ports[WORKER_NUM] = {4411, 4412, 4413, 4414};

char* local_ips[WORKER_NUM] = {"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"};
int local_ports[WORKER_NUM] = {5511, 5512, 5513, 5514};


int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);



int main(int argc, const char * argv[])
{

    int thread_id = atoi(argv[1]);
    std::thread send_thread(sendTd, thread_id);
    send_thread.detach();

    std::thread recv_thread(recvTd, thread_id);
    recv_thread.detach();

    while (1 == 1)
    {

    }
    return 0;
}
int wait4connection(char*local_ip, int local_port)
{
    int fd = socket(PF_INET, SOCK_STREAM , 0);
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(local_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, local_ip, &address.sin_addr);
    //设置socket buffer大小
    int recvbuf = 4096;
    int len = sizeof( recvbuf );
    setsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, sizeof( recvbuf ) );
    getsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, ( socklen_t* )&len );
    printf( "the receive buffer size after settting is %d\n", recvbuf );
    //绑定ip和端口
    int check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
    assert(check_ret >= 0);

    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);

    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    return connfd;

}
void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];

    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    assert(fd >= 0);

    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    int sendbuf = 4096;
    int len = sizeof( sendbuf );
    //转换成网络地址
    address.sin_port = htons(remote_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, remote_ip, &address.sin_addr);
    do
    {
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    assert(check_ret >= 0);
    //发送数据
    const char* normal_data = "my boy!";

    int ret = send(fd, normal_data, strlen(normal_data), 0);
    if (ret >= 0 )
    {
        printf("send success \n");
    }

}
void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );
    while (1 == 1)
    {
        int expected_len = 15;
        char* sockBuf = (char*)malloc(expected_len);
        int ret = recv(connfd, sockBuf, expected_len, 0);
        if (ret > 0)
        {
            printf("sockBuf=%s\n", sockBuf);
        }
    }
}


