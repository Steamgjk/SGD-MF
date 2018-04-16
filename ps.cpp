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
#include <atomic>
using namespace std;
#define BUFFER_SIZE 1024
#define WORKER_NUM 4
char* local_ips[WORKER_NUM] = {"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"};
int local_ports[WORKER_NUM] = {4411, 4412, 4413, 4414};
char* remote_ips[WORKER_NUM] = {"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"};
int remote_ports[WORKER_NUM] = {5511, 5512, 5513, 5514};

#define N  8 //用户数
#define M  8 //物品数
#define K  2 //主题个数

double R[N][M];
double P[N][K];
double Q[K][M];
bool worker_debug = false;
bool main_debug = false;
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
struct Block Pblocks[WORKER_NUM];
struct Block Qblocks[WORKER_NUM];
struct Updates Pupdts[WORKER_NUM];
struct Updates Qupdts[WORKER_NUM];




int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void printBlockPair(Block& pb, Block& qb, int minK);
double CalcRMSE();
void partitionP(int portion_num,  Block* Pblocks);
void partitionQ(int portion_num,  Block* Qblocks);

atomic_int recvCount(0);
bool canSend = false;
int worker_pidx[WORKER_NUM];
int worker_qidx[WORKER_NUM];

int main(int argc, const char * argv[])
{
    //int connfd = wait4connection(ips[0], ports[0]);
    //printf("connfd=%d\n", connfd);
    for (int i = 0; i < WORKER_NUM; i++)
    {
        worker_pidx[i] = worker_qidx[i] = i;
    }
    for (int send_thread_id = 0; send_thread_id < WORKER_NUM; send_thread_id++)
    {
        std::thread send_thread(sendTd, send_thread_id);
        send_thread.detach();
    }
    for (int recv_thread_id = 0; recv_thread_id < WORKER_NUM; recv_thread_id++)
    {
        std::thread recv_thread(recvTd, recv_thread_id);
        recv_thread.detach();
    }
    while (1 == 1)
    {

        partitionP(WORKER_NUM, Pblocks);
        partitionQ(WORKER_NUM, Qblocks);

        srand(time(0));
        bool ret = false;
        if (rand() % 2 == 0)
        {
            ret =  next_permutation(worker_pidx, worker_pidx + WORKER_NUM);
            if (!ret)
            {
                prev_permutation(worker_pidx, worker_pidx + WORKER_NUM);
            }
        }
        else
        {
            ret =  prev_permutation(worker_pidx, worker_pidx + WORKER_NUM);
            if (!ret)
            {
                next_permutation(worker_pidx, worker_pidx + WORKER_NUM);
            }
        }
        canSend = true;

        while (recvCount != WORKER_NUM)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        if (recvCount == WORKER_NUM)
        {
            printf("Collect All, Can Update\n");
            int idx = 0;
            for (int kk = 0; kk < WORKER_NUM; kk++)
            {
                //Update P [N*K]
                for (int ii = 0; ii < Pupdts[kk].eles.size(); ii++)
                {
                    int row_idx = (ii + idx) / K;
                    int col_idx = (ii + idx) % K;
                    P[row_idx][col_idx] += Pupdts[kk].eles[ii];
                }
            }
            for (int kk = 0; kk < WORKER_NUM; kk++)
            {
                //Update Q[K*M]
                for (int ii = 0; ii < Qupdts[kk].eles.size(); ii++)
                {
                    int col_idx = (ii + idx) / K;
                    int row_idx = (ii + idx) % K;
                    Q[row_idx][col_idx] += Qupdts[kk].eles[ii];
                }
            }
            printf("Update Finish, Can Distribute\n");
        }
    }

    return 0;
}
void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];

    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    printf("fd = %d\n", fd);
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
    printf("connected %s  %d\n", remote_ip, remote_port );
    while (1 == 1)
    {
        if (!canSend)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        else
        {
            size_t struct_sz = sizeof( Pblocks[send_thread_id]);
            size_t data_sz = sizeof(double) * Pblocks[send_thread_id].eles.size();
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pblocks[send_thread_id]), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pblocks[send_thread_id].eles[0]), data_sz);
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("send success \n");
            }
            free(buf);

            struct_sz = sizeof( Qblocks[send_thread_id]);
            data_sz = sizeof(double) * Qblocks[send_thread_id].eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qblocks[send_thread_id]), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qblocks[send_thread_id].eles[0]), data_sz);
            ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("send success \n");
            }
            free(buf);
        }
    }

}

void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );

    while (1 == 1)
    {
        size_t expected_len = sizeof(Updates);
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
        struct Updates* updt = (struct Updates*)(void*)sockBuf;
        int block_id = updt->block_id;
        Pupdts[block_id].block_id = block_id;
        Pupdts[block_id].clock_t = updt->clock_t;
        Pupdts[block_id].ele_num = updt->ele_num;
        Pupdts[block_id].eles.resize(updt->ele_num);
        free(sockBuf);

        size_t data_sz = sizeof(double) * (updt->ele_num);
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
        for (int i = 0; i < updt->ele_num; i++)
        {
            Pupdts[block_id].eles[i] = data_eles[i];
        }
        free(data_eles);


        expected_len = sizeof(Updates);
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
        updt = (struct Updates*)(void*)sockBuf;
        block_id = updt->block_id;
        Qupdts[block_id].block_id = block_id;
        Qupdts[block_id].clock_t = updt->clock_t;
        Qupdts[block_id].ele_num = updt->ele_num;
        Qupdts[block_id].eles.resize(updt->ele_num);
        free(sockBuf);

        data_sz = sizeof(double) * (updt->ele_num);
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
        for (int i = 0; i < updt->ele_num; i++)
        {
            Qupdts[block_id].eles[i] = data_eles[i];
        }
        free(data_eles);



        recvCount++;
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
    printf("get connection from %s  %d\n", inet_ntoa(addressClient.sin_addr), addressClient.sin_port);
    return connfd;

}




void printBlockPair(Block& pb, Block& qb, int minK)
{
    double rmse = 0.0;
    printf("\n********Below P[%d]*************\n", pb.block_id);
    for (int i = 0 ; i < pb.height; i++)
    {
        for (int j = 0; j < minK; j++)
        {
            printf("%lf\t", pb.eles[i * minK + j]);
        }
        printf("\n");
    }
    printf("\n+++++++++++Below Q[%d]+++++++++++\n", qb.block_id);
    for (int i = 0 ; i < qb.height; i++)
    {
        for (int j = 0; j < minK; j++)
        {
            printf("%lf\t", qb.eles[i * minK + j]);
        }
        printf("\n");
    }
    printf("\n++++++++Below Rb++++++++++\n");

    for (int i = 0; i < pb.height; i++)
    {
        for (int j = 0; j < qb.height; j++)
        {
            double e = 0;
            for (int k = 0; k < minK; k++)
            {
                e += pb.eles[i * minK + k] * qb.eles[j * minK + k];
            }
            printf("%lf\t", e);
        }
        printf("\n");
    }
    printf("\n++++++++Below R+++++++++++++\n");

    int row_idx = pb.sta_idx;
    int col_idx = qb.sta_idx;

    for (int i = 0; i < pb.height; i++)
    {
        for (int j = 0; j < qb.height; j++)
        {
            double e = 0;
            for (int k = 0; k < minK; k++)
            {
                e += pb.eles[i * minK + k] * qb.eles[j * minK + k];
            }
            int row_idx = i + pb.sta_idx;
            int col_idx = j + qb.sta_idx;
            printf("%lf\t", R[row_idx][col_idx]);
            rmse += (e - R[row_idx][col_idx]) *  (e - R[row_idx][col_idx]);
        }
        printf("\n");
    }
    printf("\n***************************\n");
    printf("rmse=%lf\n", rmse);

}

double CalcRMSE()
{
    double rmse = 0;
    for (int i = 0; i < N; i++)
    {
        for (int j = 0; j < M; j++)
        {
            double sum = 0;
            for (int k = 0; k < K; k++)
            {
                sum += P[i][k] * Q[k][j];
            }
            rmse += (sum - R[i][j]) * (sum - R[i][j]);
        }
    }
    rmse /= (N * M);
    rmse = sqrt(rmse);
    return rmse;
}
void partitionP(int portion_num,  Block* Pblocks)
{
    int i = 0;
    int height = N / portion_num;
    int last_height = N - (portion_num - 1) * height;

    for (i = 0; i < portion_num; i++)
    {
        Pblocks[i].block_id = i;
        Pblocks[i].data_age = 0;
        Pblocks[i].eles.clear();
        Pblocks[i].height = height;
        int sta_idx = i * height;
        if ( i == portion_num - 1)
        {
            Pblocks[i].height = last_height;
        }
        Pblocks[i].sta_idx = sta_idx;

        for (int h = 0; h < Pblocks[i].height; h++)
        {
            for (int j = 0; j < K; j++)
            {
                Pblocks[i].eles.push_back(P[h][j]);
            }
        }
    }

}

void partitionQ(int portion_num,  Block* Qblocks)
{
    int i = 0;
    int height = M / portion_num;
    int last_height = M - (portion_num - 1) * height;

    for (i = 0; i < portion_num; i++)
    {
        Qblocks[i].block_id = i;
        Qblocks[i].data_age = 0;
        Qblocks[i].eles.clear();
        Qblocks[i].height = height;
        int sta_idx = i * height;
        if ( i == portion_num - 1)
        {
            Qblocks[i].height = last_height;
        }
        Qblocks[i].sta_idx = sta_idx;

        for (int h = 0; h < Qblocks[i].height; h++)
        {
            for (int j = 0; j < K; j++)
            {
                Qblocks[i].eles.push_back(Q[j][h]);
            }
        }
    }

}