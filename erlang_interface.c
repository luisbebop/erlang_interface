#include "erlang_interface.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

// socket handle
int socket_handle;

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

void hex_dump(unsigned char *data, int size, char *caption)
{
	int i; // index in data...
	int j; // index in line...
	char temp[8];
	char buffer[128];
	char *ascii;

	memset(buffer, 0, 128);

	printf("---------> %s <--------- (%d bytes from %p)\n", caption, size, data);

	// Printing the ruler...
	printf("        +0          +4          +8          +c            0   4   8   c   \n");

	// Hex portion of the line is 8 (the padding) + 3 * 16 = 52 chars long
	// We add another four bytes padding and place the ASCII version...
	ascii = buffer + 58;
	memset(buffer, ' ', 58 + 16);
	buffer[58 + 16] = '\n';
	buffer[58 + 17] = '\0';
	buffer[0] = '+';
	buffer[1] = '0';
	buffer[2] = '0';
	buffer[3] = '0';
	buffer[4] = '0';
	for (i = 0, j = 0; i < size; i++, j++)
	{
		if (j == 16)
		{
			printf("%s", buffer);
			memset(buffer, ' ', 58 + 16);

			sprintf(temp, "+%04x", i);
			memcpy(buffer, temp, 5);

			j = 0;
		}

		sprintf(temp, "%02x", 0xff & data[i]);
		memcpy(buffer + 8 + (j * 3), temp, 2);
		if ((data[i] > 31) && (data[i] < 127))
			ascii[j] = data[i];
		else
			ascii[j] = '.';
	}

	if (j != 0)
		printf("%s", buffer);
}

#define LOWORD(l)           ((unsigned short)((l) & 0xffff))
#define HIWORD(l)           ((unsigned short)((l) >> 16))
#define LOBYTE(w)           ((unsigned char)((w) & 0xff))
#define HIBYTE(w)           ((unsigned char)((w) >> 8))
#define HH(x)				HIBYTE(HIWORD( x ))
#define HL(x)				LOBYTE(HIWORD( x ))
#define LH(x)				HIBYTE(LOWORD( x ))
#define LL(x)				LOBYTE(LOWORD( x ))
#define MAKEWORD(a, b)      ((unsigned short)(((unsigned char)((a) & 0xff)) | ((unsigned short)((unsigned char)((b) & 0xff))) << 8))
#define MAKELONG(low,high)	((int)(((unsigned short)(low)) | (((unsigned int)((unsigned short)(high))) << 16)))

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

int UCLSend(unsigned char * buf, int len)
{
	int n;
	n = write(socket_handle,buf,len);
  if (n < 0) 
  	error("ERROR writing to socket");
	return n;
}

int UCLReceive(unsigned char * buf, int maxlen)
{
	int n;
	n = read(socket_handle,buf,maxlen);
  if (n < 0) 
  	error("ERROR reading from socket");
	return n;
}

// -1: erro de comunicação
// -2: erro na resposta do mapreduce
// -3: erro ao abrir arquivo para escrita
int riak_mapreduce_request(	char * bucket_name, char * key, char * erlang_module, char * map_function, 
														char * serial_terminal, char * versao_walk, char * filename, char * crc_file, char * posxml_buffer, 
														char * response, char * save_to_file, int * ret_code)
{
	char buf[2048];
	char packet_send[2048];
	char packet_recv[1024];
	int index = 0, index_packet = 0, recvd = 0, size_to_receive = 0, total_size = 0, i = 0, size_block = 0, len_recv = 0, ret_size = 0;
	FILE *fp = NULL;
	
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
	ei_encode_binary(buf, &index, bucket_name, strlen(bucket_name));
	// elemento binário key
	ei_encode_binary(buf, &index, key, strlen(key));
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
	ei_encode_atom(buf,&index,erlang_module);
	// terceiro atom do segundo elemento, Function
	ei_encode_atom(buf,&index,map_function);
	
	// terceiro elemento, uma list com parâmetros do walk
	ei_encode_list_header(buf, &index, 5);
	// elemento binário serialterminal
	ei_encode_binary(buf, &index, serial_terminal, strlen(serial_terminal));
	// elemento binário versão walk
	ei_encode_binary(buf, &index, versao_walk, strlen(versao_walk));
	// elemento binário nomeaplicativo
	ei_encode_binary(buf, &index, filename, strlen(filename));
	// elemento binário crc aplicativo
	ei_encode_binary(buf, &index, crc_file, strlen(crc_file));
	// elemento binário buffer do posxml
	ei_encode_binary(buf, &index, posxml_buffer, strlen(posxml_buffer));
	// fim da list com parâmetros do walk
	ei_encode_list_header(buf, &index, 0);
	
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
		
	// sending buffer ...
	hex_dump(packet_send,index_packet+4,"sending");	
	if (UCLSend((unsigned char *)packet_send, index_packet+4) <= 0)
	{
		return -1;
	}
	
	// receiving buffer ...
	recvd = UCLReceive((unsigned char *)packet_recv, sizeof(packet_recv));
	hex_dump(packet_recv, recvd, "received");
	if (recvd <= 0)
	{
		return -1;
	}
		
	// verifica o buffer recebido pelo pos enviado pelo Riak
	if ((unsigned char)packet_recv[9] != 0x83 || 
			(unsigned char)packet_recv[10] != 0x6C || 
			(unsigned char)packet_recv[14] != 0x02 )  
	{
		return -2;
	}
	
	// coloca o código de retorno da mensagem de mapreduce na variavel retcode
	*ret_code = packet_recv[16];
	
	// calcula o tamanho do buffer a ser recebido
	size_to_receive = MAKELONG(MAKEWORD(packet_recv[21],packet_recv[20]),MAKEWORD(packet_recv[19],packet_recv[18]));
	ret_size = size_to_receive;
	
	// subtrai o tamanho do header antes de chegar no elemento da list q contém o arquivo binário
	recvd -= 22 /* header size */ + 1 /* ultimo byte contendo o final da list */;
		
	// subtrai do tamanho de bytes a receber, com o tamanho do header e ultimo byte recebido
	size_to_receive -= recvd;

	// download de arquivo ou copia para buffer de memoria
	if (save_to_file)
	{
			// arquivo tem tamanho maior q 0
			if (ret_size > 0)
			{
				fp = fopen(save_to_file, "wb");
				if (fp == NULL) return -3;
				if (size_to_receive <= 0)	fwrite(&packet_recv[22],1,recvd - 1,fp);
				else											fwrite(&packet_recv[22],1,recvd,fp);
			}
	}
	else
	{
		memcpy(response,&packet_recv[22], recvd);
	}
		
	// loop se necessario para baixar o restante do arquivo ou buffer
	i = 0;
	while(size_to_receive > 0)
	{
		if(size_to_receive > 1024)	size_block = 1024;
		else												size_block = size_to_receive;
		
		memset(packet_recv,0,sizeof(packet_recv));
		len_recv = UCLReceive((unsigned char *)&packet_recv[0],size_block);
		if(len_recv <= 0)
		{
			if (save_to_file && fp) fclose(fp);
			return -1;
		}

		if (save_to_file && fp)
		{
			if ((size_to_receive - len_recv) <= 0)
				fwrite(packet_recv,1,len_recv - 1,fp);
			else
				fwrite(packet_recv,1,len_recv,fp);
		}
		else 
		{
			memcpy(&response[total_size],packet_recv,len_recv);
		}

		size_to_receive -= len_recv;
		total_size += len_recv;
		i++;
	}
	
	if (save_to_file && fp) fclose(fp);
	return ret_size;
}

int main(int argc, char *argv[])
{
	char buf[2048];
	int ret;
	int ret_code = 0;
	
	memset(buf,0,sizeof(buf));

	// connecting to host ... setting global handler
	socket_handle = connect_(argc,argv);
	
	// // calling mapreduce_request to get a file, posxml: baixaarquivo
	// riak_mapreduce_request(	"assets", "wallpaper.bmp", "walk", "get_asset", 									// bucket, key, module, function
	// 												"000-000-000", "3.01", "inicio.posxml", "FFFF", "0,AAAAA,err,sn", // serial, version, app, crc, buffer
	// 												buf, NULL, &ret_code);
	
	// calling mapreduce_request to get an app, posxml: 
	ret = riak_mapreduce_request(	"terminals", "tbk_00001", "walk", "request", 											// bucket, key, module, function
													"000-000-000", "3.01", "inicio.posxml", "FFFF", "0,AAAAA,err,sn", // serial, version, app, crc, buffer
													buf, NULL, &ret_code);
													
	printf("ret = %d ret_code = %d\n", ret, ret_code);
	hex_dump(buf, ret, "response");		
	return 0;
}