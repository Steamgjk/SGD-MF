//
//  main.cpp
//  linux_socket_api
//
//  Created by Jinkun Geng on 18/05/11.
//  Copyright (c) 2016年 Jinkun Geng. All rights reserved.
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
#include <fstream>
#include <sys/time.h>
#include <map>
#include "server_rdma_op.h"
#include "client_rdma_op.h"
using namespace std;
#define CAP 200
//#define FILE_NAME "./netflix_row.txt"
//#define TEST_NAME "./test_out.txt"
//#define N  17770 // row number
//#define M  2649429 //col number
//#define K  40 //主题个数

//#define FILE_NAME "./movielen10M_train.txt"
//#define TEST_NAME "./movielen10M_test.txt"
/*
#define FILE_NAME "./mdata/traina-"
#define TEST_NAME "./mdata/testa-"
#define N 71567
#define M 65133
#define K  40 //主题个数
**/
#define BLOCK_MEM_SZ (250000000)
#define MEM_SIZE (BLOCK_MEM_SZ*4*2)
char* to_send_block_mem;
char* to_recv_block_mem;


#define FILE_NAME "./data/TrainingMap-"
#define TEST_NAME "./data/TestMap-"
#define N 1000000
#define M 1000000
#define K  100 //主题个数

#define QP_GROUP 1
int send_round_robin_idx = 0;
int recv_round_robin_idx = 0;

int WORKER_NUM = 4;
char* local_ips[CAP] = {"12.12.10.18", "12.12.10.18", "12.12.10.18", "12.12.10.18"};
int local_ports[CAP] = {4411, 4412, 4413, 4414};
char* remote_ips[CAP] = {"12.12.10.12", "12.12.10.15", "12.12.10.16", "12.12.10.17"};
int remote_ports[CAP] = {5511, 5512, 5513, 5514};

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
    bool isP;
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
        ele_num = bitem.ele_num;
        sta_idx = bitem.sta_idx;
        return *this;
    }
    void printBlock()
    {

        printf("block_id  %d\n", block_id);
        printf("data_age  %d\n", data_age);
        printf("ele_num  %d\n", ele_num);
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
        ele_num = uitem.ele_num;
        eles = uitem.eles;
        return *this;
    }

    void printUpdates()
    {
        printf("update block_id %d\n", block_id );
        printf("clock_t  %d\n", clock_t);
        printf("ele size %ld\n", ele_num);
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");
    }
};
struct Block Pblocks[CAP];
struct Block Qblocks[CAP];
struct Updates Pupdts[CAP];
struct Updates Qupdts[CAP];


void WriteLog(Block&Pb, Block&Qb, int iter_cnt);
int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void rdma_sendTd(int send_thread_id);
void rdma_recvTd(int recv_thread_id);
void partitionP(int portion_num,  Block* Pblocks);
void partitionQ(int portion_num,  Block* Qblocks);
void InitFlag();

atomic_int recvCount(0);
bool canSend[CAP] = {false};
int worker_pidx[CAP];
int worker_qidx[CAP];

long long time_span[300];

int main(int argc, const char * argv[])
{
    send_round_robin_idx = 0;
    recv_round_robin_idx = 0;
    for (int i = 0; i < CAP; i++)
    {
        local_ports[i] = 4411 + i;
        remote_ports[i] = 5511 + i;
    }
    to_send_block_mem = (void*)malloc(MEM_SIZE);
    to_recv_block_mem = (void*)malloc(MEM_SIZE);
    printf("to_send_block_mem=%p  to_recv_block_mem=%p\n", to_send_block_mem, to_recv_block_mem );
    InitFlag();


    //gen P and Q
    if (argc == 2)
    {
        WORKER_NUM = atoi(argv[1]) ;
    }

    for (int gp = 0; gp < QP_GROUP; gp++)
    {
        for (int recv_thread_id = 0; recv_thread_id < WORKER_NUM; recv_thread_id++)
        {
            int thid = recv_thread_id + gp * WORKER_NUM;
            std::thread recv_thread(rdma_recvTd, thid);
            recv_thread.detach();
        }
    }



    printf("wait for you for 3s\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    for (int gp = 0; gp < QP_GROUP; gp++)
    {
        for (int send_thread_id = 0; send_thread_id < WORKER_NUM; send_thread_id++)
        {
            int thid = send_thread_id + gp * WORKER_NUM;
            std::thread send_thread(rdma_sendTd, thid);
            send_thread.detach();
        }
    }

    printf("wait for 5s\n");

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    srand(1);
    //LoadTestRating();
    //printf("Load Complete\n");
    printf("start work\n");
    partitionP(WORKER_NUM, Pblocks);
    partitionQ(WORKER_NUM, Qblocks);
    for (int i = 0; i < WORKER_NUM; i++)
    {
        for (int j = 0; j < Pblocks[i].ele_num; j++)
        {
            //Pblocks[i].eles[j] = drand48() * 0.6;
            Pblocks[i].eles[j] = drand48() * 0.6;
        }
        for (int j = 0; j < Qblocks[i].ele_num; j++)
        {
            //Qblocks[i].eles[j] = drand48() * 0.6;
            Qblocks[i].eles[j] = drand48() * 0.6;
        }
    }

    for (int i = 0; i < WORKER_NUM; i++)
    {
        canSend[i] = false;
    }
    for (int i = 0; i < WORKER_NUM; i++)
    {
        worker_pidx[i] = worker_qidx[i] = i;
    }

    /*
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
    **/



    /*
        std::thread send_thread(rdma_sendTd, 2);
        send_thread.detach();
        std::thread recv_thread(rdma_recvTd, 2);
        recv_thread.detach();
    **/

    int iter_t = 0;

    for (int i = 0; i < WORKER_NUM; i++)
    {
        worker_pidx[i] = i;
        worker_qidx[i] = 3 - i;
    }
    struct timeval beg, ed;

    while (1 == 1)
    {
        srand(time(0));
        bool ret = false;
        random_shuffle(worker_pidx, worker_pidx + WORKER_NUM); //迭代器
        random_shuffle(worker_qidx, worker_qidx + WORKER_NUM); //迭代器


        for (int i = 0; i < WORKER_NUM; i++)
        {
            printf("%d  [%d:%d]\n", i, worker_pidx[i], worker_qidx[i] );
        }


        for (int i = 0; i < WORKER_NUM; i++)
        {
            canSend[i] = true;
        }
        printf("canSend! flag ok\n");
        while (recvCount != WORKER_NUM)
        {
            //cout << "RecvCount\t" << recvCount << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }


        if (iter_t == 0)
        {
            gettimeofday(&beg, 0);
        }
        if (recvCount == WORKER_NUM)
        {
            if (iter_t % 10 == 0)
            {
                gettimeofday(&ed, 0);
                /*
                                for (int bid = 0; bid < WORKER_NUM; bid++)
                                {

                                    WriteLog(Pblocks[bid], Qblocks[bid], iter_t);
                                }
                **/

                time_span[iter_t / 10] = (ed.tv_sec - beg.tv_sec) * 1000000 + ed.tv_usec - beg.tv_usec;
                printf("%d\t%lld\n", iter_t, time_span[iter_t / 10] );

            }
            //printf("iter_t=%d\n", iter_t );
            send_round_robin_idx = (send_round_robin_idx + 1) % QP_GROUP;
            recv_round_robin_idx = (recv_round_robin_idx + 1) % QP_GROUP;

            recvCount = 0;
        }
        iter_t++;
        if (iter_t == 1010)
        {
            for (int i = 0; i < 102; i++)
            {
                printf("%lld\n", time_span[i] );
            }
        }
    }

    return 0;
}

void InitFlag()
{
    size_t offset = 0;
    char* sta = to_recv_block_mem;
    Block* bk = NULL;
    for (int i = 0; i < 8; i++)
    {
        offset = i * BLOCK_MEM_SZ;
        sta = to_recv_block_mem + offset;
        bk = (Block*)(void*)sta;
        bk->block_id = -1;
    }
}
void WriteLog(Block & Pb, Block & Qb, int iter_cnt)
{
    char fn[100];
    sprintf(fn, "./Rtrack/Pblock-%d-%d", iter_cnt, Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    printf("fn:%s\n", fn );
    sprintf(fn, "./Rtrack/Qblock-%d-%d", iter_cnt, Qb.block_id);
    ofstream qofs(fn, ios::trunc);
    for (int h = 0; h < Qb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            qofs << Qb.eles[h * K + j] << " ";
        }
        qofs << endl;
    }
    printf("fn:%s\n", fn );
    //getchar();
}

void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];

    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    //printf("fd = %d\n", fd);
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
        printf("[Td:%d] trying to connect %s %d\n", send_thread_id, remote_ip, remote_port );
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    assert(check_ret >= 0);
    printf("[Td:%d]connected %s  %d\n", send_thread_id, remote_ip, remote_port );
    while (1 == 1)
    {

        if (canSend[send_thread_id])
        {
            int pbid = worker_pidx[send_thread_id];
            int qbid = worker_qidx[send_thread_id];
            size_t struct_sz = sizeof( Pblocks[pbid]);
            size_t data_sz = sizeof(double) * Pblocks[pbid].eles.size();
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pblocks[pbid]), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pblocks[pbid].eles[0]), data_sz);
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Td:%d] send success pbid =%d ret=%d\n", send_thread_id, pbid, ret );
            }
            free(buf);

            struct_sz = sizeof( Qblocks[qbid]);
            data_sz = sizeof(double) * Qblocks[qbid].eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qblocks[qbid]), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qblocks[qbid].eles[0]), data_sz);
            ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Td:%d] send success qbid=%d ret =%d\n", send_thread_id, qbid, ret);
            }
            free(buf);
            canSend[send_thread_id] = false;
        }
    }

}

void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );
    while (1 == 1)
    {
        printf("recving ...\n");
        struct timeval st, et, tspan;
        gettimeofday(&st, 0);
        size_t expected_len = sizeof(Block);
        char* sockBuf = (char*)malloc(expected_len);
        size_t cur_len = 0;
        int ret = 0;
        while (cur_len < expected_len)
        {
            //printf("[Td:%d] cur_len = %ld expected_len-cur_len = %ld\n", recv_thread_id, cur_len, expected_len - cur_len );
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret <=  0)
            {
                printf("Mimatch! %d\n", ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            //printf("ret=%d\n", ret );
            cur_len += ret;
            //printf("cur_len=%d expected_len=%d\n", cur_len, expected_len );
        }
        //printf("come here\n");
        struct Block* pb = (struct Block*)(void*)sockBuf;
        //pb->printBlock();
        size_t data_sz = sizeof(double) * (pb->ele_num);
        char* dataBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        //printf("pb ele_num %d\n", pb->ele_num );
        while (cur_len < data_sz)
        {
            ret = recv(connfd, dataBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
            // printf("cur_len=%d data_sz=%d\n", cur_len, data_sz );
        }

        double* data_eles = (double*)(void*)dataBuf;
        int block_idx = pb->block_id ;
        Pblocks[block_idx].block_id = pb->block_id;
        Pblocks[block_idx].sta_idx = pb->sta_idx;
        Pblocks[block_idx].height = pb->height;
        Pblocks[block_idx].ele_num = pb->ele_num;
        Pblocks[block_idx].eles.resize(pb->ele_num);
        Pblocks[block_idx].isP = pb->isP;
        for (int i = 0; i < pb->ele_num; i++)
        {
            Pblocks[block_idx].eles[i] = data_eles[i];
        }
        free(sockBuf);
        free(dataBuf);

        printf("successful rece one Block data_sz = %ld block_sz=%ld\n", data_sz, expected_len);
        expected_len = sizeof(Block);
        sockBuf = (char*)malloc(expected_len);
        cur_len = 0;
        ret = 0;
        while (cur_len < expected_len)
        {
            printf("[Td:%d] cur_len = %ld expected_len-cur_len = %ld\n", recv_thread_id, cur_len, expected_len - cur_len );
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret <=  0)
            {
                printf("Mimatch! %d\n", ret);
                if (ret == 0)
                {
                    exit(-1);
                }
            }
            cur_len += ret;
        }
        pb = (struct Block*)(void*)sockBuf;
        data_sz = sizeof(double) * (pb->ele_num);
        dataBuf = (char*)malloc(data_sz);
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, dataBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        data_eles = (double*)(void*)dataBuf;
        block_idx = pb->block_id ;
        Qblocks[block_idx].block_id = pb->block_id;
        Qblocks[block_idx].sta_idx = pb->sta_idx;
        Qblocks[block_idx].height = pb->height;
        Qblocks[block_idx].ele_num = pb->ele_num;
        Qblocks[block_idx].eles.resize(pb->ele_num);
        Qblocks[block_idx].isP = pb->isP;
        for (int i = 0; i < pb->ele_num; i++)
        {
            Qblocks[block_idx].eles[i] = data_eles[i];
        }

        printf("successful rece another Block\n");
        free(sockBuf);
        free(dataBuf);
        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
        printf("recv success time = %lld\n", mksp );
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
    //int recvbuf = 4096;
    //int len = sizeof( recvbuf );
    //setsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, sizeof( recvbuf ) );
    //getsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, ( socklen_t* )&len );
    //printf( "the receive buffer size after settting is %d\n", recvbuf );
    //绑定ip和端口
    int check_ret = -1;
    do
    {
        printf("binding... %s  %d\n", local_ip, local_port);
        check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret >= 0);

    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);
    printf("listening... %s  %d\n", local_ip, local_port);
    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    printf("get connection from %s  %d\n", inet_ntoa(addressClient.sin_addr), addressClient.sin_port);
    return connfd;

}



void partitionP(int portion_num,  Block * Pblocks)
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
        //printf("i-%d sta_idx-%d\n", i, sta_idx );
        Pblocks[i].ele_num = Pblocks[i].height * K;
        Pblocks[i].eles.resize(Pblocks[i].ele_num);
    }

}

void partitionQ(int portion_num,  Block * Qblocks)
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
        Qblocks[i].ele_num = Qblocks[i].height * K;
        Qblocks[i].eles.resize(Qblocks[i].ele_num);

    }

}




void rdma_sendTd(int send_thread_id)
{

    printf("ps  send_thread_id=%d\n", send_thread_id);
    //printf("ps send waiting for 3s...\n");
    //std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    char* remote_ip = remote_ips[send_thread_id % WORKER_NUM];
    int remote_port = remote_ports[send_thread_id];
    struct sockaddr_in server_sockaddr;
    int ret, option;
    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    get_addr(remote_ip, (struct sockaddr*) &server_sockaddr);
    server_sockaddr.sin_port = htons(remote_port);
    client_rdma_op cro;
    printf("prepare conn remote_ip=%s  remote_port=%d\n", remote_ip, remote_port);
    ret = cro.client_prepare_connection(&server_sockaddr);
    if (ret)
    {
        rdma_error("Failed to setup client connection , ret = %d \n", ret);
        return ret;
    }

    ret = cro.client_pre_post_recv_buffer();
    if (ret)
    {
        rdma_error("Failed to setup client connection , ret = %d \n", ret);
        return ret;
    }
    ret = cro.client_connect_to_server();
    if (ret)
    {
        rdma_error("Failed to setup client connection , ret = %d \n", ret);
        return ret;
    }
    printf("[%d] connect  ok\n", send_thread_id);

    size_t offset = (send_thread_id % WORKER_NUM) * BLOCK_MEM_SZ * 2;
    char* buf = to_send_block_mem + offset;
    ret = cro.client_send_metadata_to_server1(buf, BLOCK_MEM_SZ * 2);
    if (ret)
    {
        rdma_error("Failed to setup client connection , ret = %d \n", ret);
        return ret;
    }
    printf("[%d]client_send_metadata_to_server1  ok\n", send_thread_id);
    /*
    while (1 == 1)
    {
        printf("rdma_sendTd\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    **/

    while (1 == 1)
    {

        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        while (canSend[send_thread_id % WORKER_NUM] == false)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (send_round_robin_idx != send_thread_id / WORKER_NUM)
        {
            printf("[%d]:send_round_robin_idx=%d  s=%d\n", send_thread_id, send_round_robin_idx,  send_thread_id / WORKER_NUM);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }
        else
        {
            printf("[%d]:Me:send_round_robin_idx=%d  s=%d\n", send_thread_id, send_round_robin_idx,  send_thread_id / WORKER_NUM);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        printf("[%d]  canSend? %d\n", send_thread_id, canSend[send_thread_id] );
        if (canSend[send_thread_id % WORKER_NUM] == true)
        {
            printf("[%d] canSend\n",  send_thread_id);
            //getchar();
            int pbid = worker_pidx[send_thread_id % WORKER_NUM];
            int qbid = worker_qidx[send_thread_id % WORKER_NUM];
            printf("pbid=%d  qbid=%d sid=%d\n", pbid, qbid, send_thread_id % WORKER_NUM );
            size_t struct_sz = sizeof( Pblocks[pbid]);
            size_t data_sz = sizeof(double) * Pblocks[pbid].eles.size();
            size_t total_len = struct_sz + data_sz;
            //printf("[%d] canSend check 1\n",  send_thread_id);
            memcpy(buf, &(Pblocks[pbid]), struct_sz);
            //printf("[%d] canSend check 2\n",  send_thread_id);
            memcpy(buf + struct_sz, (char*) & (Pblocks[pbid].eles[0]), data_sz);
            //printf("start send...\n");
            ret = cro.start_remote_write(total_len, 0);
            if (ret == 0)
            {
                printf("[Td:%d] send success pbid=%d isP=%d ret =%d\n", send_thread_id, pbid, Pblocks[pbid].isP, ret);
            }
            else
            {
                printf("fail ret=%d\n", ret );
            }

            struct_sz = sizeof( Qblocks[qbid]);
            data_sz = sizeof(double) * Qblocks[qbid].eles.size();
            total_len = struct_sz + data_sz;

            memcpy(buf, &(Qblocks[qbid]), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qblocks[qbid].eles[0]), data_sz);
            ret = cro.start_remote_write(total_len, BLOCK_MEM_SZ);
            if (ret == 0 )
            {
                printf("[Td:%d] send success qbid=%d isP=%d ret =%d\n", send_thread_id, qbid, Qblocks[qbid].isP, ret);
            }
            canSend[send_thread_id % WORKER_NUM] = false;
        }

    }

    return ret;


}
void rdma_recvTd(int recv_thread_id)
{
    printf("ps rdma_recv thread_id = %d\n local_ip=%s  local_port=%d\n", recv_thread_id, local_ips[recv_thread_id % WORKER_NUM], local_ports[recv_thread_id]);
    char* buf = to_recv_block_mem + (recv_thread_id % WORKER_NUM) * BLOCK_MEM_SZ * 2;

    server_rdma_op sro;

    int ret = sro.rdma_server_init(local_ips[recv_thread_id % WORKER_NUM], local_ports[recv_thread_id], buf, BLOCK_MEM_SZ * 2);
    /*
    while (1 == 1)
    {
        printf("before enter rdma_recvTd\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    **/
    while (1 == 1)
    {
        if (recv_round_robin_idx != recv_thread_id / WORKER_NUM)
        {
            continue;
        }
        //printf("recving ...\n");
        struct timeval st, et, tspan;
        gettimeofday(&st, 0);
        struct Block * pb = (struct Block*)(void*)buf;
        //printf("ps: pb blockid =%d\n", pb->block_id);
        while (pb->block_id < 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        int block_idx = pb->block_id ;
        Pblocks[block_idx].block_id = pb->block_id;
        Pblocks[block_idx].sta_idx = pb->sta_idx;
        Pblocks[block_idx].height = pb->height;
        Pblocks[block_idx].ele_num = pb->ele_num;
        Pblocks[block_idx].eles.resize(pb->ele_num);
        Pblocks[block_idx].isP = pb->isP;
        size_t struct_sz = sizeof(Block);
        double*data_eles = (double*)(void*) (buf + struct_sz);
        for (int i = 0; i < pb->ele_num; i++)
        {
            Pblocks[block_idx].eles[i] = data_eles[i];
        }

        //printf("successful reve one Block id=%d data_ele=%d\n", pb->block_id, pb->ele_num);
        pb->block_id = -1;

        pb = (struct Block*)(void*)(buf + BLOCK_MEM_SZ);

        while (pb->block_id < 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        block_idx = pb->block_id ;
        Qblocks[block_idx].block_id = pb->block_id;
        Qblocks[block_idx].sta_idx = pb->sta_idx;
        Qblocks[block_idx].height = pb->height;
        Qblocks[block_idx].ele_num = pb->ele_num;
        Qblocks[block_idx].eles.resize(pb->ele_num);
        Qblocks[block_idx].isP = pb->isP;
        for (int i = 0; i < pb->ele_num; i++)
        {
            Qblocks[block_idx].eles[i] = data_eles[i];
        }

        //printf("successful rece another Block\n");

        pb->block_id = -1;

        gettimeofday(&et, 0);
        long long mksp = (et.tv_sec - st.tv_sec) * 1000000 + et.tv_usec - st.tv_usec;
        //printf("recv success time = %lld\n", mksp );
        recvCount++;
    }
}