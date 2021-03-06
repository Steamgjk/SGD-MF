

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
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <map>
using namespace std;





char* local_ips[10] = {"12.12.10.12", "12.12.10.15", "12.12.10.16", "12.12.10.17"};
int local_ports[10] = {5511, 5512, 5513, 5514};


#define FILE_NAME "./traina.txt"
#define TEST_NAME "./testa.txt"
#define N 71567
#define M 65133
#define K  40 //主题个数

#define CAP 30
#define PERIOD 4
int WORKER_NUM = 2;
#define ThreshIter 5
#define SEQ_LEN 5000

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
//struct Block Pblock;
//struct Block Qblock;
struct Block Pblocks[CAP];
struct Block Qblocks[CAP];

struct timeval start, stop, diff;

atomic_int recvedCount(0);
atomic_int toSendCount(0);
int worker_pidx[CAP];
int worker_qidx[CAP];
int send_pidx;
int send_qidx;
int recv_pidx;
int recv_qidx;
int directions[4] = {1, 1, 0, 0};

int block_seq[SEQ_LEN];
// i is right and 0 is up
int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void partitionP(int portion_num,  Block* Pblocks);
void partitionQ(int portion_num,  Block* Qblocks);
void submf(Block& minP, Block& minQ, int minK, float alpha = 0.003, float beta = 0.1);
void  FilterDataSet(map<long, double>& RTestMap, long row_sta_idx, long row_len, long col_sta_idx, long col_len);
void WriteLog(Block&Pb, Block&Qb, int iter_cnt);
void getMinR(double* minR, int row_sta_idx, int row_len, int col_sta_idx, int col_len);
void LoadRating();
void LoadTestRating();
double CalcRMSE(map<long, double>& TestMap, Block & minP, Block & minQ);

map<long, double> RMap;
map<long, double> TestMap;
int thread_id = -1;
int main(int argc, const char * argv[])
{
    int iter_thresh = 10;
    srand(time(0));
    thread_id = atoi(argv[1]);
    WORKER_NUM = atoi(argv[2]);
    if (argc >= 4)
    {
        iter_thresh = atoi(argv[3]);
    }
    LoadRating();
    LoadTestRating();

    std::thread send_thread(sendTd, thread_id);
    send_thread.detach();

    std::thread recv_thread(recvTd, thread_id);
    recv_thread.detach();
    for (int i = 0; i < WORKER_NUM; i++)
    {
        worker_pidx[i] =  i;
        worker_qidx[i] = WORKER_NUM - 1 - i ;
        //p->up
        //q->right
    }
    send_pidx = worker_pidx[thread_id];
    send_qidx = worker_qidx[thread_id];
    recv_pidx = worker_pidx[(thread_id + WORKER_NUM - 1) % WORKER_NUM];
    recv_qidx = worker_qidx[(thread_id + WORKER_NUM - 1) % WORKER_NUM];

    partitionP(WORKER_NUM, Pblocks);
    partitionQ(WORKER_NUM, Qblocks);

    for (int i = 0; i < WORKER_NUM; i++)
    {
        Pblocks[i].isP = true;
        Qblocks[i].isP = false;
    }
    for (int i = 0; i < Pblocks[worker_pidx[thread_id]].ele_num; i++)
    {
        Pblocks[worker_pidx[thread_id]].eles[i] = drand48() * 0.6;
    }
    for (int j = 0; j < Qblocks[worker_qidx[thread_id]].ele_num; j++)
    {
        Qblocks[worker_qidx[thread_id]].eles[j] =  drand48() * 0.6;
    }
    recvedCount++;
    int cnt = 0;
    int iter_cnt = 0;
    bool isstart = false;
    while (1 == 1)
    {
        if (recvedCount > 0)
        {
            if (!isstart)
            {
                isstart = true;
                std::this_thread::sleep_for(std::chrono::milliseconds(5000));
                printf("OK Start\n");
                gettimeofday(&start, 0);
            }
            //SGD
            int pidx = worker_pidx[thread_id];
            int qidx = worker_qidx[thread_id];
            recvedCount--;
            int row_sta_idx = Pblocks[pidx].sta_idx;
            int row_len = Pblocks[pidx].height;
            int col_sta_idx = Qblocks[qidx].sta_idx;
            int col_len = Qblocks[qidx].height;

            //printf("processing p %d q %d\n", pidx, qidx );

            submf( Pblocks[pidx], Qblocks[qidx], K);

            iter_cnt++;
            if (iter_cnt == iter_thresh)
            {
                gettimeofday(&stop, 0);

                long long mksp = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
                printf("itercnt = %d  time = %lld\n", iter_cnt, mksp);
                WriteLog(Pblocks[pidx], Qblocks[qidx], iter_cnt);
                exit(0);
            }


            int direct = directions[cnt % PERIOD];

            if (direct == 1)
            {
                //I will go right
                for (int i = 0; i < WORKER_NUM; i++)
                {
                    worker_qidx[i] = (worker_qidx[i] + 1) % WORKER_NUM;
                }
            }
            else
            {
                //I will go up
                for (int i = 0; i < WORKER_NUM; i++)
                {
                    worker_pidx[i] = (worker_pidx[i] + WORKER_NUM - 1) % WORKER_NUM;
                }
            }
            cnt++;
            toSendCount++;

        }
    }

}

void LoadRating()
{
    ifstream ifs(FILE_NAME);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", FILE_NAME);
        exit(-1);
    }
    int cnt = 0;
    int temp = 0;
    long hash_idx = 0;
    double ra = 0;
    while (!ifs.eof())
    {
        ifs >> hash_idx >> ra;
        RMap.insert(pair<long, double>(hash_idx, ra));
        //KeyVec.insert(hash_idx);
        cnt++;
        if (cnt % 1000000 == 0)
        {
            printf("cnt=%d\n", cnt );
        }
    }

    printf("cnt=%d sizeof(long)=%ld\n", cnt, sizeof(long));
}

void LoadTestRating()
{
    ifstream ifs(TEST_NAME);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", TEST_NAME);
        exit(-1);
    }
    int cnt = 0;
    int temp = 0;
    long hash_idx = 0;
    double ra = 0;
    while (!ifs.eof())
    {
        ifs >> hash_idx >> ra;
        TestMap.insert(pair<long, double>(hash_idx, ra));
        cnt++;
        if (cnt % 10000 == 0)
        {
            printf("cnt = %ld\n", cnt );
        }
    }
}

void getMinR(double* minR, int row_sta_idx, int row_len, int col_sta_idx, int col_len)
{
    //printf("row_sta_idx = %d row_len=%d col_sta_idx=%d  col_len = %d\n", row_sta_idx, row_len, col_sta_idx, col_len);

    ifstream ifs(FILE_NAME);
    string temp;
    for (int i = 0; i < row_sta_idx; i++)
    {
        getline(ifs, temp);
        //cout << "temp:\t" << temp << endl;
    }
    //printf("check cc 1\n");
    int line_no = row_sta_idx;
    double temp_db;
    int total_num = row_len * col_len;
    int cnt = 0;
    //printf("check cc 2\n");

    for (int i = row_sta_idx; i < row_sta_idx + row_len; i++)
    {
        for (int j = 0 ; j < col_sta_idx; j++)
        {
            ifs >> temp_db;
            //cout << "tf " << temp_db << endl;
        }
        //cout << endl;
        for (int j = col_sta_idx; j < col_sta_idx + col_len; j++)
        {
            ifs >> minR[cnt];
            //cout << "minR " << minR[cnt] << endl;
            cnt++;
        }
        //cout << endl;
        //getchar();
        for (int j = col_sta_idx + col_len; j < M; j++)
        {
            ifs >> temp_db;
            //cout << "tfb " << temp_db << endl;
            //getchar();
        }
        //getchar();
    }
    //printf("Returned  \n");
}

void WriteLog(Block&Pb, Block&Qb, int iter_cnt)
{
    char fn[100];
    sprintf(fn, "./track/Pblock-%d-%d", iter_cnt, Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    sprintf(fn, "./track/Qblock-%d-%d", iter_cnt, Qb.block_id);
    ofstream qofs(fn, ios::trunc);
    for (int h = 0; h < Qb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            qofs << Qb.eles[h * K + j] << " ";
        }
        qofs << endl;
    }
}



double CalcRMSE(map<long, double>& RTestMap, Block & minP, Block & minQ)
{
    //printf("calc RMSE-1debug...\n");
    double rmse = 0;
    int cnt = 0;
    map<long, double>::iterator iter;
    int positve_cnt = 0;
    int negative_cnt = 0;
    long row_sta_idx = minP.sta_idx;
    long col_sta_idx = minQ.sta_idx;
    for (iter = RTestMap.begin(); iter != RTestMap.end(); iter++)
    {
        long real_hash_idx = iter->first;
        long row_idx = real_hash_idx / M - row_sta_idx;
        long col_idx = real_hash_idx % M - col_sta_idx;
        double sum = 0;

        for (int k = 0; k < K; k++)
        {
            //sum += P[row_idx][k] * Q[k][col_idx];
            /*
            if (row_idx * K + k > minP.eles.size() || col_idx * K + k > minQ.eles.size() )
            {
                printf("Psz %ld  idx %ld  Qsz %ld  idx %ld  real_hash_idx %ld row_idx %ld col_idx %ld Pblock_id %d QblockId %d Psta  %d Qsta %d\n", minP.eles.size(), row_idx * K + k ,   minQ.eles.size(), col_idx * K + k, real_hash_idx, row_idx, col_idx, minP.block_id, minQ.block_id, minP.sta_idx, minQ.sta_idx );
                getchar();
            }
            **/
            sum += minP.eles[row_idx * K + k] * minQ.eles[col_idx * K + k];
            //printf("k=%d  Pv %lf  Qv %lf  sum=%lf\n", k, minP.eles[row_idx * K + k],  minQ.eles[col_idx * K + k], sum);
        }
        /*
                if (sum > iter->second)
                {
                    positve_cnt++;
                }
                else
                {
                    negative_cnt++;
                    //printf("sum = %lf  real=%lf\n", sum, iter->second );
                }
                **/
        rmse += (sum - iter->second) * (sum - iter->second);
        cnt++;
    }
    //printf("RTestMap sz %ld cnt = %d\n", RTestMap.size(), cnt );
    if (cnt != 0)
    {
        rmse /= cnt;
        rmse = sqrt(rmse);
    }
    else
    {
        rmse = 0;
    }

    //printf("positve_cnt=%d negative_cnt=%d\n", positve_cnt, negative_cnt );
    return rmse;
}

void  FilterDataSet(map<long, double>& RTestMap, long row_sta_idx, long row_len, long col_sta_idx, long col_len)
{
    //printf("Entering FilterDataSet\n");
    std::map<long, double>::iterator iter;
    long mem_cnt = 0;
    for (iter = TestMap.begin(); iter != TestMap.end(); iter++)
    {
        long hash_idx = iter->first;
        long r_idx = hash_idx / M;
        long c_idx = hash_idx % M;
        if (row_sta_idx <= r_idx && r_idx < row_sta_idx + row_len && col_sta_idx <= c_idx && c_idx < col_sta_idx + col_len)
        {
            RTestMap.insert(pair<long, double>(iter->first, iter->second));
        }


    }

    // printf("Entering Test FilterDataSet  %ld\n", RTestMap.size());


}


void submf(Block & minP, Block & minQ,  int minK,  float alpha , float beta)
{
    //printf("begin submf\n");
    double error = 0;
    int minN = minP.height;
    int minM = minQ.height;
    int row_sta_idx = minP.sta_idx;
    int col_sta_idx = minQ.sta_idx;
    int row_len = minP.height;
    int col_len = minQ.height;

    int Psz =  minP.height * minK;
    int Qsz = minQ.height * minK;
    //printf("row_sta_idx %ld row_len=%ld  col_sta_idx %ld col_len=%ld\n", row_sta_idx, row_len, col_sta_idx, col_len );


    std::map<long, double> RTestMap;
    FilterDataSet(RTestMap, row_sta_idx, row_len, col_sta_idx, col_len);

    double old_rmse = CalcRMSE(RTestMap, minP, minQ);
    double new_rmse = old_rmse;
    int iter_cnt = 0;
    vector<double> originalP = minP.eles;
    vector<double> originalQ = minQ.eles;
    while ( new_rmse > 0.999 * old_rmse )
    {
        vector<double> oldP = minP.eles;
        vector<double> oldQ = minQ.eles;
        //printf("row_sta_idx %ld row_len=%ld  col_sta_idx %ld col_len=%ld\n", row_sta_idx, row_len, col_sta_idx, col_len );
        //printf("oldP  sz %ld bid %ld oldQ sz  %ld  bid %ld\n", oldP.size(), minP.block_id, oldQ.size(), minQ.block_id );
        for (int c_row_idx = 0; c_row_idx < row_len; c_row_idx++)
        {

            long i = c_row_idx;
            long j = rand() % col_len;

            long real_row_idx = i + row_sta_idx;
            long real_col_idx = j + col_sta_idx;
            long real_hash_idx = real_row_idx * M + real_col_idx;

            map<long, double>::iterator iter;
            iter = RMap.find(real_hash_idx);
            if (iter != RMap.end())
            {
                error = iter->second;
                for (int k = 0; k < minK; ++k)
                {
                    /*
                    if (i * minK + k >= oldP.size() || j * minK + k >= oldQ.size())
                    {

                        //printf("i=%ld minK=%d j =%ld k=%d  idx = %ld idx =%ld Psz=%ld Qsz =%ld\n", i, minK, j, k, i * minK + k, j * minK + k, oldP.size(), oldQ.size() );
                        printf("P(1) is %d  Q(0) is %d\n", minP.isP, minQ.isP);
                        getchar();
                    }
                    **/
                    error -= oldP[i * minK + k] * oldQ[j * minK + k];
                }

                for (int k = 0; k < minK; ++k)
                {
                    minP.eles[i * minK + k] += alpha * (error * oldQ[j * minK + k] - beta * oldP[i * minK + k]);
                    minQ.eles[j * minK + k] += alpha * (error * oldP[i * minK + k] - beta * oldQ[j * minK + k]);

                }
            }
        }
        iter_cnt++;
        new_rmse = CalcRMSE(RTestMap, minP, minQ);
        /*
        if (iter_cnt % 100 == 0)
        {
            printf("old_rmse = %lf new_rmse=%lf itercnt=%d\n", old_rmse, new_rmse, iter_cnt );
        }
        **/
        if (iter_cnt > 100)
        {
            break;
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

    //绑定ip和端口
    int check_ret = -1;
    do
    {
        printf("binding...\n");
        check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret >= 0);
    printf("bind ok\n");
    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);

    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);

    printf("thread %d listening at %s %d\n", thread_id, local_ip, local_port );
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    return connfd;

}
void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    int right_idx = (send_thread_id + 1) % WORKER_NUM;
    char* remote_ip = local_ips[right_idx];
    int remote_port = local_ports[right_idx];

    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    assert(fd >= 0);

    struct sockaddr_in address;
    bzero(&address, sizeof(address));

    //转换成网络地址
    address.sin_port = htons(remote_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, remote_ip, &address.sin_addr);
    do
    {
        printf("Trying to connect to %s %d\n", remote_ip, remote_port);
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    //发送数据
    printf("connect to %s %d\n", remote_ip, remote_port);
    int send_cnt = 0;
    while (1 == 1)
    {
        if (toSendCount > 0)
        {
            int direct = directions[send_cnt % PERIOD];
            size_t struct_sz = sizeof(Block);
            Block* tosend_block = NULL;
            size_t data_sz = 0;
            char*buf = NULL;
            if (direct == 1)
            {
                // 1 is right,
                //I will go right and the pre node will come to this column from left, so I need to send him my Qblock
                //printf("SendTd direct=1\n");
                data_sz = sizeof(double) * Qblocks[send_qidx].eles.size();
                buf = (char*)malloc(struct_sz + data_sz);
                //printf("SendTd direct=1 ch1... eles=%ld\n", Qblocks[send_qidx].eles.size() );
                memcpy(buf, &(Qblocks[send_qidx]), struct_sz);
                memcpy(buf + struct_sz, (char*) & (Qblocks[send_qidx].eles[0]), data_sz);
                //updat send_qidx to right
                send_qidx = (send_qidx + 1) % WORKER_NUM;
                //printf("SendTd direct=1 ch3\n");
                //printf("send Q\n");
                //getchar();
            }
            else
            {
                //printf("SendTd direct=0\n");
                data_sz = sizeof(double) * Pblocks[send_pidx].eles.size();
                buf = (char*)malloc(struct_sz + data_sz);
                memcpy(buf, &(Pblocks[send_pidx]), struct_sz);
                memcpy(buf + struct_sz, (char*) & (Pblocks[send_pidx].eles[0]), data_sz);
                //update send_pidx to up
                send_pidx = (send_pidx + WORKER_NUM - 1) % WORKER_NUM;
                //printf("send P\n");
                //getchar();
            }
            //printf("SendTd  check point 1\n");
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            /*
            if (ret >= 0 )
            {
                printf("[Id:%d] send success \n", thread_id);
            }
            **/
            free(buf);
            send_cnt++;
            toSendCount--;
            //printf("Send return loop...\n");
            //getchar();
        }
    }

}
void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );

    printf("[Td:%d] worker get connection\n", recv_thread_id);
    int recv_cnt = 0;
    while (1 == 1)
    {
        int direct = directions[recv_cnt % PERIOD];
        size_t expected_len = sizeof(Block);
        char* sockBuf = (char*)malloc(expected_len);
        size_t cur_len = 0;
        int ret = 0;
        //printf("recv Check 1\n");
        while (cur_len < expected_len)
        {
            //printf("cur_len = %ld  expected_len = %ld\n", cur_len, expected_len);
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            //printf("check 1.5\n");
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        struct Block* pb = (struct Block*)(void*)sockBuf;
        if (direct == 1)
        {
            //printf("recv direct=1\n");
            // I will go right, so I send my Qblock to the right neighbor,
            //similarly, I also receive a Qblock from my left neighbor
            Qblocks[recv_qidx].block_id = pb->block_id;
            Qblocks[recv_qidx].sta_idx = pb->sta_idx;
            Qblocks[recv_qidx].height = pb->height;
            Qblocks[recv_qidx].ele_num = pb->ele_num;
            Qblocks[recv_qidx].isP = pb->isP;
            //printf("should be Q (0) real is %d\n", pb->isP );
        }
        else
        {
            Pblocks[recv_pidx].block_id = pb->block_id;
            Pblocks[recv_pidx].sta_idx = pb->sta_idx;
            Pblocks[recv_pidx].height = pb->height;
            Pblocks[recv_pidx].ele_num = pb->ele_num;
            Pblocks[recv_pidx].isP = pb->isP;
            //printf("should be P(1) real is %d\n", pb->isP );
        }

        size_t data_sz = sizeof(double) * (pb->ele_num);
        sockBuf = (char*)malloc(data_sz);
        //printf("recv check 4\n");
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
                exit(-1);
            }
            cur_len += ret;
        }
        double* data_eles = (double*)(void*)sockBuf;


        if (direct == 1)
        {
            for (int i = 0; i < Qblocks[recv_qidx].ele_num; i++)
            {
                Qblocks[recv_qidx].eles[i] = data_eles[i];
            }
            // I will go right, so I send my Qblock to the right neighbor,
            //similarly, I also receive a Qblock from my left neighbor

            //I will go right, update recv_qidx
            recv_qidx = (recv_qidx + 1) % WORKER_NUM;

        }
        else
        {
            for (int i = 0; i < Pblocks[recv_pidx].ele_num; i++)
            {
                Pblocks[recv_pidx].eles[i] = data_eles[i];
            }
            // I will go up, update recv_pidx
            recv_pidx = (recv_pidx + WORKER_NUM - 1) % WORKER_NUM;
        }
        recv_cnt++;
        free(data_eles);
        recvedCount++;
        //printf("recv pausing..\n");
        //getchar();
    }
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
        /*
                for (int h = 0; h < Pblocks[i].height; h++)
                {
                    for (int j = 0; j < K; j++)
                    {
                        Pblocks[i].eles.push_back(P[h][j]);
                    }
                }
                **/
        Pblocks[i].ele_num = Pblocks[i].height * K;
        Pblocks[i].eles.resize(Pblocks[i].ele_num);
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
        /*
                for (int h = 0; h < Qblocks[i].height; h++)
                {
                    for (int j = 0; j < K; j++)
                    {
                        Qblocks[i].eles.push_back(Q[j][h]);
                    }
                }
                **/
        Qblocks[i].ele_num = Qblocks[i].height * K;
        Qblocks[i].eles.resize(Qblocks[i].ele_num);

    }

}
