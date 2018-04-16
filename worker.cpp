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


struct Block
{
    int block_id;
    int data_age;
    int sta_idx;
    int height; //height
    int ele_num;
    vector<double> eles;
    Block()
    {

    }
    Block operator=(Block& bitem)
    {
        block_id = bitem.block_id;
        data_age = bitem.data_age;
        height = bitem.height;
        eles = bitem.eles;
        sta_idx = bitem.sta_idx;
        return *this;
    }
    void printBlock()
    {
        printf("block_id  %d\n", block_id);
        printf("data_age  %d\n", data_age);
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");
    }
};
struct Updates
{
    int block_id;
    int clock_t;
    int ele_num;
    vector<double> eles;
    Updates()
    {

    }
    Updates operator=(Updates& uitem)
    {
        block_id = uitem.block_id;
        clock_t = uitem.clock_t;
        eles = uitem.eles;
        return *this;
    }

    void printUpdates()
    {
        printf("update block_id %d\n", block_id );
        printf("clock_t  %d\n", clock_t);
        printf("ele size %ld\n", eles.size());
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");
    }
};
struct Block Pblock;
struct Block Qblock;
struct Updates Pupdt;
struct Updates Qupdt;

bool canSend = false;
bool hasRecved = false;

int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);

void submf(double *minR, Block& minP, Block& minQ, Updates& updateP, Updates& updateQ, int minN, int minM, int minK, int steps, float alpha, float beta);



int main(int argc, const char * argv[])
{

    int thread_id = atoi(argv[1]);
    std::thread send_thread(sendTd, thread_id);
    send_thread.detach();

    std::thread recv_thread(recvTd, thread_id);
    recv_thread.detach();

    while (1 == 1)
    {
        if (hasRecved)
        {
            //SGD
            int row_sta_idx = Pblock.sta_idx;
            int row_len = Pblock.height;
            int col_sta_idx = Qblock.sta_idx;
            int col_len = Qblock.height;
            canSend = true;
            hasRecved = false;
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }
    return 0;
}


void submf(double *minR, Block& minP, Block& minQ, Updates& updateP, Updates& updateQ, int minN, int minM, int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02)
{

    double error = 0;
    int Psz =  minP.height * minK;
    int Qsz = minQ.height * minK;
    updateP.eles.resize(Psz);
    updateQ.eles.resize(Qsz);
    int ii = 0;
    for (ii = 0; ii < Psz; ii++)
    {
        updateP.eles[ii] = 0;
    }
    for ( ii = 0; ii < Qsz; ii++)
    {
        updateQ.eles[ii] = 0;
    }

    //for (int step = 0; step < steps; ++step)
    {
        for (int i = 0; i < minN; ++i)
        {
            for (int j = 0; j < minM; ++j)
            {

                if (minR[i * minM + j] > 0)
                {
                    //printf("idx = %d\n", i * minM + j );
                    //这里面的error 就是公式6里面的e(i,j)
                    error = minR[i * minM + j];
                    //printf("error = %lf\n", error );
                    for (int k = 0; k < minK; ++k)
                    {
                        //error_m -= P[i * minK + k] * Q[k * minM + j];
                        error -= minP.eles[i * minK + k] * minQ.eles[j * minK + k];
                    }
                    //更新公式6
                    for (int k = 0; k < minK; ++k)
                    {
                        updateP.eles[i * minK + k] += alpha * (2 * error * minQ.eles[j * minK + k] - beta * minP.eles[i * minK + k]);
                        updateQ.eles[j * minK + k] += alpha * (2 * error * minP.eles[i * minK + k] - beta * minQ.eles[j * minK + k]);

                    }

                }
            }
        }


    }



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

    while (1 == 1)
    {
        if (!canSend)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        else
        {
            size_t struct_sz = sizeof(Pupdt);
            size_t data_sz = sizeof(double) * Pupdt.eles.size();
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pupdt), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pupdt.eles[0]), data_sz);
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("send success \n");
            }
            free(buf);

            struct_sz = sizeof(Qupdt);
            data_sz = sizeof(double) * Qupdt.eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qupdt), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qupdt.eles[0]), data_sz);
            ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("send success \n");
            }
            free(buf);
            canSend = false;
        }
    }

}
void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );
    while (1 == 1)
    {
        size_t expected_len = sizeof(Pblock);
        char* sockBuf = (char*)malloc(expected_len);
        size_t cur_len = 0;
        int ret = 0;
        while (cur_len < expected_len)
        {
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        struct Block* pb = (struct Block*)(void*)sockBuf;
        Pblock.block_id = pb->block_id;
        Pblock.data_age = pb->data_age;
        Pblock.sta_idx = pb->sta_idx;
        Pblock.height = pb->height;
        Pblock.ele_num = pb->ele_num;
        Pblock.eles.resize(pb->ele_num);
        free(sockBuf);

        size_t data_sz = sizeof(double) * (pb->ele_num);
        sockBuf = (char*)malloc(data_sz);

        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        double* data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < pb->ele_num; i++)
        {
            Pblock.eles[i] = data_eles[i];
        }
        free(data_eles);



        expected_len = sizeof(Pblock);
        sockBuf = (char*)malloc(expected_len);
        cur_len = 0;
        ret = 0;
        while (cur_len < expected_len)
        {
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        struct Block* qb = (struct Block*)(void*)sockBuf;
        Qblock.block_id = qb->block_id;
        Qblock.data_age = qb->data_age;
        Qblock.sta_idx = qb->sta_idx;
        Qblock.height = qb->height;
        Qblock.ele_num = qb-> ele_num;
        Qblock.eles.resize(qb->ele_num);
        free(sockBuf);

        data_sz = sizeof(double) * (qb-> ele_num);
        sockBuf = (char*)malloc(data_sz);

        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < qb->ele_num; i++)
        {
            Qblock.eles[i] = data_eles[i];
        }
        free(data_eles);

        hasRecved = true;


    }
}


