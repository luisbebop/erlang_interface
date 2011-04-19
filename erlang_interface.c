#include "erlang_interface.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

int ei_encode_list_header(char *buf, int *index, int arity)
{
  char *s = buf + *index;
  char *s0 = s;

  if (arity < 0) return -1;
  else if (arity > 0) {
    if (!buf) s += 5;
    else {
      put8(s,ERL_LIST_EXT);
      put32be(s,arity);
    }
  }
  else {
    /* empty list */
    if (!buf) s++;
    else put8(s,ERL_NIL_EXT);
  }

  *index += s-s0; 

  return 0;
}

int ei_encode_tuple_header(char *buf, int *index, int arity)
{
  char *s = buf + *index;
  char *s0 = s;
  
  if (arity < 0) return -1;

  if (arity <= 0xff) {
    if (!buf) s += 2;
    else {
      put8(s,ERL_SMALL_TUPLE_EXT);
      put8(s,arity);
    }
  }
  else {
    if (!buf) s += 5;
    else {
      put8(s,ERL_LARGE_TUPLE_EXT);
      put32be(s,arity);
    }
  }

  *index += s-s0; 

  return 0;
}

int ei_encode_binary(char *buf, int *index, const void *p, long len)
{
  char *s = buf + *index;
  char *s0 = s;

  if (!buf) s += 5;
  else {
    put8(s,ERL_BINARY_EXT);
    put32be(s,len);
    memmove(s,p,len);
  }
  s += len;
  
  *index += s-s0; 

  return 0; 
}

int ei_encode_atom_len(char *buf, int *index, const char *p, int len)
{
  char *s = buf + *index;
  char *s0 = s;

  /* This function is documented to truncate at MAXATOMLEN (256) */ 
  if (len > MAXATOMLEN)
    len = MAXATOMLEN;

  if (!buf) s += 3;
  else {
    put8(s,ERL_ATOM_EXT);
    put16be(s,len);

    memmove(s,p,len); /* unterminated string */
  }
  s += len;

  *index += s-s0; 

  return 0; 
}

int ei_encode_atom(char *buf, int *index, const char *p)
{
    return ei_encode_atom_len(buf, index, p, strlen(p));
}

int ei_encode_long(char *buf, int *index, long p)
{
  char *s = buf + *index;
  char *s0 = s;

  if ((p < 256) && (p >= 0)) {
    if (!buf) s += 2;
    else {
      put8(s,ERL_SMALL_INTEGER_EXT);
      put8(s,(p & 0xff));
    }
  }
  else if ((p <= ERL_MAX) && (p >= ERL_MIN)) {
    /* FIXME: Non optimal, could use (p <= LONG_MAX) && (p >= LONG_MIN)
       and skip next case */
    if (!buf) s += 5;
    else {
      put8(s,ERL_INTEGER_EXT);
      put32be(s,p);
    }
  }
  else {
    if (!buf) s += 7;
    else {
      put8(s,ERL_SMALL_BIG_EXT);
      put8(s,4);	         /* len = four bytes */
      put8(s, p < 0);            /* save sign separately */
      
			// problem here. Some plataforms don't have abs ... :(
	  	//put32le(s, abs(p));        /* OBS: Little Endian, and p now positive */
	  	put32le(s,p);
    }
  }
  
  *index += s-s0; 

  return 0; 
}

/* add the version identifier to the start of the buffer */
int ei_encode_version(char *buf, int *index)
{
  char *s = buf + *index;
  char *s0 = s;

  if (!buf) s ++;
  else put8(s,(unsigned char)ERL_VERSION_MAGIC);
  *index += s-s0;
  
  return 0;
}

// util functions

char *upb_put_v_uint64_t(char *buf, unsigned long val)
{
  do {
    unsigned char byte = val & 0x7f;
    val >>= 7;
    if(val) byte |= 0x80;
    *buf++ = byte;
  } while(val);
  return buf;
}

char *upb_put_v_uint32_t(char *buf, unsigned int val)
{
  return upb_put_v_uint64_t(buf, val);
}

int pb_add_request(char *buf, int *index, const void *p, long len)
{
  char *s = buf + *index;
  char *s0 = s;

  if (!buf) s ++;
  else {
		put8(s,(unsigned char)0x17);
		put8(s,(unsigned char)0x0A);
		s = upb_put_v_uint32_t(s, len);
		memmove(s,p,len);
	}
	s += len;
	
  *index += s-s0;
  
  return 0;
}

int pb_add_content_type(char * buf, int *index)
{
	char *s = buf + *index;
  char *s0 = s;
	char p[28];
	int len;
	
	memset(p,0,sizeof(p));
	strcpy(p,"application/x-erlang-binary");
	len = strlen(p);

  if (!buf) s ++;
  else {
    put8(s,0x12);
		put8(s,0x1B);
    memmove(s,p,len);
  }
  s += len;
  
  *index += s-s0; 

  return 0;
}

void hexdump(unsigned char *buffer, int size)
{
	unsigned long i;

	for (i=0;i<size;i++)
	{
		printf("%02X ",buffer[i]);
	}
	printf("\n");
}

#define LOWORD(l)           ((unsigned short)((l) & 0xffff))
#define HIWORD(l)           ((unsigned short)((l) >> 16))
#define LOBYTE(w)           ((unsigned char)((w) & 0xff))
#define HIBYTE(w)           ((unsigned char)((w) >> 8))
#define HH(x)				HIBYTE(HIWORD( x ))
#define HL(x)				LOBYTE(HIWORD( x ))
#define LH(x)				HIBYTE(LOWORD( x ))
#define LL(x)				LOBYTE(LOWORD( x ))

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

int connect_(int argc, char *argv[])
{
    int sockfd, portno, n;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    if (argc < 3) {
       fprintf(stderr,"usage %s hostname port\n", argv[0]);
       exit(0);
    }
    portno = atoi(argv[2]);
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) error("ERROR opening socket");
    server = gethostbyname(argv[1]);
    if (server == NULL) {
        fprintf(stderr,"ERROR, no such host\n");
        exit(0);
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(portno);
    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) 
    		error("ERROR connecting");

    //close(sockfd);
    return sockfd;
}

int send_(int sockfd, unsigned char * buf, int len)
{
	int n;
	n = write(sockfd,buf,len);
  if (n < 0) 
  	error("ERROR writing to socket");
}

int recv_(int sockfd, unsigned char * buf, int maxlen)
{
	int n;
	n = read(sockfd,buf,maxlen);
  if (n < 0) 
  	error("ERROR reading from socket");
	return n;
}

int main(int argc, char *argv[])
{
	char buf[2048];
	char packet_send[2048];
	char packet_recv[2048];
	int index = 0, index_packet = 0, socket = 0, recvd = 0;
	
	memset(buf,0,sizeof(buf));
	memset(packet_send,0,sizeof(packet_send));
	memset(packet_recv,0,sizeof(packet_recv));
	
	// version
	ei_encode_version(buf,&index);
	
	// list do mapreduce contendo 3 tuples
	ei_encode_list_header(buf, &index, 3);
	
	// tuple 1: inputs
	ei_encode_tuple_header(buf,&index, 2);
	// atom inputs
	ei_encode_atom(buf,&index,"inputs");
	// list do inputs
	ei_encode_list_header(buf, &index, 1);
	// tuple contendo 2 elementos binários, bucket and key
	ei_encode_tuple_header(buf,&index, 2);
	// elemento binário bucket
	ei_encode_binary(buf, &index, "bucket", strlen("bucket"));
	// elemento binário key
	ei_encode_binary(buf, &index, "key", strlen("key"));
	// fim da list do inputs
	ei_encode_list_header(buf, &index, 0);
	
	// tuple 2: query
	ei_encode_tuple_header(buf,&index, 2);
	// atom query
	ei_encode_atom(buf,&index,"query");
	// list da query
	ei_encode_list_header(buf, &index, 1);
	// tuple contendo 4 elementos, { map, {modfun,Module,Function}, args, true }
	ei_encode_tuple_header(buf,&index, 4);
	// primeiro elemento, atom type
	ei_encode_atom(buf,&index,"map");
	// segundo elemento, nova tuple contendo 3 atoms
	ei_encode_tuple_header(buf,&index, 3);
	// primeiro atom do segundo elemento, modfun
	ei_encode_atom(buf,&index,"modfun");
	// segundo atom do segundo elemento, Module
	ei_encode_atom(buf,&index,"walk");
	// terceiro atom do segundo elemento, Function
	ei_encode_atom(buf,&index,"request");
	// terceiro elemento, args
	ei_encode_atom(buf,&index,"none");
	// quarto elemento, atom true
	ei_encode_atom(buf,&index,"true");
	// fim da list da query
	ei_encode_list_header(buf, &index, 0);
	
	// tuple 3: timeout
	ei_encode_tuple_header(buf,&index, 2);
	// atom timeout
	ei_encode_atom(buf,&index,"timeout");
	// integer timeout
	ei_encode_long(buf, &index, 5000);
	
	// fim da list do mapreduce contendo 3 tuples
	ei_encode_list_header(buf, &index, 0);
	
	// add request to protocol buffers message
	pb_add_request(&packet_send[4], &index_packet, buf, index);
	// add content_type to protocol buffers message
	pb_add_content_type(&packet_send[4], &index_packet);
	// add size to packet_send
	packet_send[0] = HH(index_packet);
	packet_send[1] = HL(index_packet);
	packet_send[2] = LH(index_packet);
	packet_send[3] = LL(index_packet);
	
	// connecting to host ...
	socket = connect_(argc,argv);
	
	// sending buffer ...
	printf("sending - size buf: %d\n",index_packet+4);
	hexdump(packet_send,index_packet+4);
	send_(socket, packet_send, index_packet+4);
	
	// receiving buffer ...
	recvd = recv_(socket, packet_recv, 1024);
	printf("received - size buf: %d\n",recvd);
	hexdump(packet_recv,recvd);
	
	return 0;
}