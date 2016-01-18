/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    //what I have added
	//      memberNode->myPos = 0;
	//      memberNode->mp1q = ; empty queue.

    initMemberListTable(memberNode);

	//and this
	/*MemberListEntry *object = new MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime());
	   Because it is dynaically allocated if we use this!!!*/

	//MemberListEntry object(id, port, memberNode->heartbeat, par->getcurrtime());-----use this or the down one

	memberNode->memberList.push_back(MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime()));
	//memberNode->memberList.push_back(object);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
	delete memberNode;
	delete par;
	delete emulNet;
	delete log;
	        //check
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();
    Address *addrr;
	//send heartbeat
	for(auto &i : memberNode->memberList){
        addrr = new Address(to_string(i.getid())+to_string(i.getport()));
        sendHeartbeat(addrr);
    }
    return;

}

void MP1Node::sendHeartbeat(Address *addrr){
	MessageHdr *msg;
#ifdef DEBUGLOG
	static char s[1024];
#endif

    size_t msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = HEARTBEAT;
	memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
		sprintf(s, "Sending the heartbeat...");
		log->LOG(&memberNode->addr, s);
#endif

		//send heartbeat
		emulNet->ENsend(&memberNode->addr, addrr, (char *)msg, msgsize );

		free(msg);

}
/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*

	I have defined a new message type HEARTBEAT in member.h file, if the message is HEARTBEAT, data would be the heartbeat. It checks the new heartbeat, if greater
	it updates the old, as well as he local time.

	*/
	/* "trying" to retrieve the data*/
    int msghdr;//changed

	char *addressFrom, *finalData, *msg, *heartbeat;
	addressFrom=(char *)malloc(sizeof(Address));
	msg = (char *) malloc(sizeof(MessageHdr));
	finalData = (char *) malloc(size);
	heartbeat = (char *) malloc(sizeof(long));
	string addrs;

	memcpy(addressFrom, data+sizeof(msghdr), sizeof(Address));//changed
	addrs=(string)addressFrom;

	memcpy((char *) msg, (data), sizeof(msghdr));
    msghdr=*(int *)msg;
	memcpy(finalData, (char *)(data+sizeof(msghdr)+sizeof(Address)), size);
    int id;
    short port;
	memcpy(&id, (char *) addressFrom, sizeof(int));
    memcpy(&port, (char *)(addressFrom+5), sizeof(short));


	//size_t pos = addrs.find(":");
    //int id = stoi(addrs.substr(0, pos));
    //short port = (short)stoi(addrs.substr(pos + 1, addrs.size()-pos-1));
    MemberListEntry j;
    for(auto &i : memberNode->memberList){
		if(i.getid() == id){
            j=i;
            break;
		}
	}

	if(msghdr==HEARTBEAT){
		if( finalData > (char *)(j.getheartbeat())){
			j.setheartbeat((long)finalData);
		}
	}

	if(msghdr == JOINREQ){

		memcpy(heartbeat, (char *)(finalData+sizeof(msghdr)) + 1 + sizeof(memberNode->addr.addr), sizeof(long));

		//send JOINREPLY message-make another function to send JOINREPLY message(contains the list).
		Address *addr=new Address(addressFrom);
		replyToJoin(addr);
        delete addr;
		//add to the membership list
		memberNode->memberList.push_back(MemberListEntry(id, port, atol(heartbeat), par->getcurrtime()));

	}

	if(msghdr == JOINREPLY){
		int i=0;
		int idR;
		short portR;
		long heartbeatR;
        MemberListEntry *ml;

		for(char *j=finalData; i < (int)string(finalData).length(); j = (char *)(j+ sizeof(MemberListEntry))){
            ml= (MemberListEntry *)j;
			i+=sizeof(MemberListEntry);
			//memcpy(idR, stoi(string(j).substr(0, 4)), sizeof(int));
			//memcpy(portR, stoi(string((char *)(j+idr))), sizeof(short));
			//memcpy(heartbeatR, stoi(string((char *)(j + idR + portR))), sizeof(long)));
			idR=ml->getid();
			portR=ml->getport();
			heartbeatR=ml->getheartbeat();

			//idR=stoi(string(j).substr(0, 4));
			//portR=(short)stoi(string((char *)(j+sizeof(idR))));

			//heartbeatR=atol((char *)(j + sizeof(int) + sizeof(short)));
			//MemberListEntry object = new MemberListEntry(idR, portR, heartbeatR, timestampR);
			memberNode->memberList.push_back(MemberListEntry(idR, portR, heartbeatR, par->getcurrtime()));

		}

	}
	//if(msg == "DUMMYLASTMSGTYPE"){
		//don't know much about this.
	//}
	return false;
}

void MP1Node::replyToJoin(Address *address) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

		size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->memberList) + sizeof(Address);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREPLY
        msg->msgType = JOINREPLY;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1), &memberNode->memberList, sizeof(memberNode->memberList));

#ifdef DEBUGLOG
        sprintf(s, "REPLY MESSAGE...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREPLY message to introducer member
        emulNet->ENsend(&memberNode->addr, address, (char *)msg, msgsize);

        free(msg);

        return;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a time out period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	 //     *******************THINGS CAN BE ADJUSTED LATER ON IF SYNTAX ERRORS ARE FOUND************************************************
	 //check the membership list
	 //memberNode contains the object of Member class as instantiated in the constructor

	int j=0;
	for(auto &i : memberNode->memberList){                             //iterating over the memberList provided by Member class
		j++;
		if(par->getcurrtime() - i.gettimestamp() > TREMOVE){
		    memberNode->memberList.erase(memberNode->memberList.begin() + j);
		}
	}

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
