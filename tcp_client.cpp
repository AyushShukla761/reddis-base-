#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <fcntl.h>
#include <cstring>
#include <vector>



using namespace std;

const size_t max_msg = 4096;

static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static int32_t read_full(int fd, char *buff, size_t n){

    while(n>0){
        ssize_t rv= read(fd, buff, n);
        if(rv<=0){
            return -1;
        }
        assert((size_t)rv<=n);
        n-=(size_t)rv;
        buff+=rv;
    }
    return 0;
}
static int32_t write_all(int fd,const char *buff, size_t n){

    while(n>0){
        ssize_t rv= write(fd, buff, n);
        if(rv<=0){
            return -1;
        }
        assert((size_t)rv<=n);
        n-=(size_t)rv;
        buff+=rv;
    }
    return 0;
}


// static void buf_append(vector<uint8_t> &buf, const uint8_t *data, size_t len) {
//     buf.insert(buf.end(), data, data + len);
// }

// the `query` function was simply splited into `send_req` and `read_res`.

static int32_t send_req(int fd, const vector<string> &cmd) {
    uint32_t len = 4;
    for (const string &s : cmd) {
        len += 4+ s.size();  // +1 for the space
    }
    if (len > max_msg) {
        return -1;
    }

    char wbuf[4 + max_msg];
    memcpy(&wbuf[0], &len, 4);
    uint32_t n= cmd.size();
    memcpy(&wbuf[4], &n, 4);
    size_t cur= 8;
    for(const string &s : cmd){
        uint32_t l= (uint32_t)s.size();
        memcpy(&wbuf[cur], &l, 4);
        memcpy(&wbuf[cur+4], s.data(), l);
        cur+= 4+s.size();
    }

    return write_all(fd, wbuf, 4+len);
}


// static int32_t query(int fd, const char *text){
//     uint32_t len = (uint32_t)strlen(text);
//     if (len > max_msg) {
//         return -1;
//     }

//     char wbuf[4+ max_msg];
//     memcpy(wbuf, &len, 4);
//     memcpy(&wbuf[4], text,len);
//     if(int32_t err =write_all(fd, wbuf,4+len)){
//         return err;
//     }


//     char rbuf[4+ max_msg];
//     errno=0;
//     int32_t err= read_full(fd, rbuf, 4);
//     if(err){
//         msg(errno==0? "EOF" : "read() error");
//         return err;
//     }
//     memcpy(&len, rbuf, 4);
//     if(len> max_msg){
//         msg("too long");
//         return -1;
//     }

//     err= read_full(fd, &rbuf[4],len);
//     if(err){
//         msg("read() error");
//         return err;
//     }

//     printf("Server says: %.*s\n", len, &rbuf[4]);
//     return 0;
// }





enum{
    TAG_NIL = 0,
    TAG_ERR = 1,
    TAG_STR = 2,
    TAG_INT = 3,
    TAG_DBL = 4,
    TAG_ARR = 5,
};

static int32_t print_response(const uint8_t *data, size_t size){
    if(size <1){
        msg("empty response");
        return -1;
    }
    switch (data[0])
    {
    case TAG_NIL:
        printf("nil\n");
        return 1;
    
    case TAG_ERR:
        if(size<1 +8){
            msg("bad error response");
            return -1;
        }
        {
            int32_t code=0;
            uint32_t len=0;
            memcpy(&code, &data[1], 4);
            memcpy(&len, &data[1+ 4], 4);
            if(size< 1+8+len){
                msg("bad error response");
                return -1;
            }
            printf("(err) %d %.*s\n", code, len, &data[1 + 8]);
            return 1+8+len;
        }
    case TAG_STR:
        if(size<1 +4){
            msg("bad string response");
            return -1;
        }
        {
            uint32_t len=0;
            memcpy(&len, &data[1], 4);
            if(size< 1+4+len){
                msg("bad string response");
                return -1;
            }
            printf("(str) %.*s\n", len, &data[1 + 4]);
            return 1+4+len;
        }
    case TAG_INT:
        if(size<1 +8){
            msg("bad int response");
            return -1;
        }
        {
            int64_t v=0;
            memcpy(&v, &data[1], 8);
            printf("(int) %ld\n", v);
            return 1+8;
        }
    case TAG_DBL:
        if(size<1 +8){
            msg("bad double response");
            return -1;
        }
        {
            double v=0;
            memcpy(&v, &data[1], 8);
            printf("(dbl) %lf\n", v);
            return 1+8;
        }
    case TAG_ARR:
        if(size<1 +4){
            msg("bad array response");
            return -1;
        }
        {
            uint32_t n=0;
            memcpy(&n, &data[1], 4);
            printf("(arr) %u\n", n);
            size_t cur= 1+4;
            for(uint32_t i=0; i<n; i++){
                int32_t err= print_response(&data[cur], size-cur);
                if(err<0){
                    return -1;
                }
                cur+= (size_t)err;
            }
            printf("(arr) end\n");
            return (int32_t)cur;
        }
    default:
        msg("unknown response type");
        return -1;
        
    }


}


static int32_t read_res(int fd){

    char rbuf[4+ max_msg+1];
    errno=0;

    int32_t err= read_full(fd,rbuf,4);

    if(err){
        if (errno == 0) {
            msg("EOF");
        } else {
            msg("read() error");
        }
        return err;
    }

    uint32_t len = 0;
    memcpy(&len, rbuf, 4);
    if (len > max_msg) {
        msg("too long");
        return -1;
    }

    
    err = read_full(fd, &rbuf[4], len);
    if (err) {
        msg("read() error");
        return err;
    }

    int32_t rv = print_response((uint8_t *)&rbuf[4], len);
    if (rv > 0 && (uint32_t)rv != len) {
        msg("bad response");
        rv = -1;
    }
    return rv;

}


int main(int argc, char **argv) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);  // 127.0.0.1
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("connect");
    }

    // int32_t err = query(fd, "hello1");
    // if (err) {
    //     goto L_DONE;
    // }
    // err = query(fd, "hello2");
    // if (err) {
    //     goto L_DONE;
    // }
    // err = query(fd, "hello3");
    // if (err) {
    //     goto L_DONE;
    // }


    // vector<string> q_list= {"hello1", "hello2", "hello3", string(max_msg,'z'),"hello5"};

    // for(const string &s  : q_list){
    //     int32_t err =send_req(fd, (uint8_t * )s.data(),s.size());
    //     if(err){
    //         goto L_DONE;
    //     }
    // }

    // for(size_t i=0; i<q_list.size();i++){
    //     int32_t err =read_res(fd);
    //     if(err){
    //         goto L_DONE;
    //     }
    // }    



    vector<string> cmd;
    for (int i = 1; i < argc; ++i) {
        cmd.push_back(argv[i]);
    }
    int32_t err = send_req(fd, cmd);
    if (err) {
        goto L_DONE;
    }
    err = read_res(fd);
    if (err) {
        goto L_DONE;
    }

L_DONE:
    close(fd);
    return 0;
}