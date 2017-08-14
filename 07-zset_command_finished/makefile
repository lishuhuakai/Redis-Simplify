CC	:= gcc
CFLAGS:= -w -std=gnu99 -ggdb -ffunction-sections 
LFLAGS	:= -lpthread
BINS 	:= redis
SRCS	:= $(wildcard *.c) # 当前目录下的所有的.c文件 
OBJS	:= $(SRCS:.c=.o) # 将所有的.c文件名替换为.o

.PHONY: all clean

all:$(BINS)

BINOS	= $(addsuffix .o, $(BINS))
TEMP_OBJ= $(filter-out $(BINOS), $^)

$(BINS):$(OBJS) 
	@echo "正在链接程序......";
	$(foreach BIN, $@, $(CC) $(CFLAGS) $(TEMP_OBJ) $(BIN).o $(LFLAGS) -o $(BIN));   

%.d:%.c
	@echo "正在生成依赖中......"; \
	rm -f $@; \
	$(CC) -MM $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

-include $(SRCS:.c=.d)

clean:
	rm -f *.o *.d
	rm -f $(BINS)

# makefile说白了就是拼凑字符串
