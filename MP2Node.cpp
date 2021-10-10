/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	int del=0;
	int del2=0;
	bool needstable=false;
	int p=1;
	for(auto i:hasMyReplicas){
		bool temp=false;
		for(auto j:curMemList){
			if(j.nodeAddress.addr==i.nodeAddress.addr)	
				temp=true;
		}
		if(!temp){
		needstable=true;
		del+=p;}
		p++;
	}
	p=1;
	for(auto i:haveReplicasOf){
		bool temp=false;
		for(auto j:curMemList){
			if(j.nodeAddress.addr==i.nodeAddress.addr)	
				temp=true;
		}
		if(!temp){
		needstable=true;
		del2+=p;}
		p++;
	}
	if(del==1)hasMyReplicas.erase(hasMyReplicas.begin(),hasMyReplicas.begin()+1);
	else if(del==2)hasMyReplicas.erase(hasMyReplicas.begin()+1,hasMyReplicas.begin()+2);
	else if(del==3)hasMyReplicas.erase(hasMyReplicas.begin()+0,hasMyReplicas.begin()+2);
	if(del2==1)haveReplicasOf.erase(haveReplicasOf.begin(),haveReplicasOf.begin()+1);
	else if(del2==2)haveReplicasOf.erase(haveReplicasOf.begin()+1,haveReplicasOf.begin()+2);
	else if(del2==3)haveReplicasOf.erase(haveReplicasOf.begin()+0,haveReplicasOf.begin()+2);
	ring=curMemList;
	if(needstable){
		
		stabilizationProtocol();
		}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
 transaction::transaction(int trans_id, int timestamp, MessageType mType, string key, string value){
	this->id = trans_id;
	this->timestamp = timestamp;
	this->replyCount = 0;
	this->successCount = 0;
	this->mType = mType;
	this->key = key;
	this->value = value;
}
transaction::transaction(int trans_id, int timestamp, MessageType mType, string key){
	this->id = trans_id;
	this->timestamp = timestamp;
	this->replyCount = 0;
	this->successCount = 0;
	this->mType = mType;
	this->key = key;
}
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}
void MP2Node::newTransaction(int trans_id, MessageType mType, string key, string value){
	int timestamp = this->par->getcurrtime();
	transaction* t = new transaction(trans_id, timestamp, mType, key, value);
	this->transMap.emplace(trans_id, t);
}
void MP2Node::newTransaction(int trans_id, MessageType mType, string key){
	int timestamp = this->par->getcurrtime();
	transaction* t = new transaction(trans_id, timestamp, mType, key);
	this->transMap.emplace(trans_id, t);
}
/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	 vector<Node>pos=findNodes(key);
	 
	 
	 for(auto n:pos){

		 Message msg (g_transID,this->memberNode->addr,CREATE,key,value);
		 newTransaction(g_transID, CREATE, key, value);
		 Address taradd=n.nodeAddress;
		 string data = msg.toString();
		 emulNet->ENsend(&memberNode->addr, &taradd,  data);
		 
		 
	 }
	 ++g_transID;
}
void MP2Node::stableCreate(string key, string value) {
	/*
	 * Implement this
	 */
	 vector<Node>pos=findNodes(key);
	 
	 
	 for(auto n:pos){

		 Message msg (-1,this->memberNode->addr,CREATE,key,value);
		 
		 Address taradd=n.nodeAddress;
		 string data = msg.toString();
		 emulNet->ENsend(&memberNode->addr, &taradd,  data);
	 }
	
}
/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	 
	 vector<Node>pos=findNodes(key);
	 
	 
	 for(auto n:pos){

		 Message msg (g_transID,this->memberNode->addr,READ,key);
		 newTransaction(g_transID, READ, key);
		 Address taradd=n.nodeAddress;string data = msg.toString();
		 emulNet->ENsend(&memberNode->addr, &taradd,  data);
		 
	 }
	 ++g_transID;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	 vector<Node>pos=findNodes(key);
	 
	 
	 for(auto n:pos){

		 Message msg (g_transID,this->memberNode->addr,UPDATE,key,value);
		 newTransaction(g_transID, UPDATE, key, value);
		 Address taradd=n.nodeAddress;
		 string data = msg.toString();
		 emulNet->ENsend(&memberNode->addr, &taradd,  data);
		 
	 }
	 ++g_transID;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	 
	 vector<Node>pos=findNodes(key);
	 
	 
	 for(auto n:pos){

		 Message msg (g_transID,this->memberNode->addr,DELETE,key);
		 newTransaction(g_transID, DELETE, key);
		 Address taradd=n.nodeAddress;
		 string data = msg.toString();
		 emulNet->ENsend(&memberNode->addr, &taradd,  data);
		 
	 }
	 ++g_transID;
	 
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica,int transID) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	vector<Node>pos=findNodes(key);
	if(!(this->memberNode->addr==pos[0].nodeAddress)){
		haveReplicasOf.push_back(pos[0]);
	}
	else{
		hasMyReplicas.push_back(pos[1]);
		hasMyReplicas.push_back(pos[2]);
	}
	if( this->ht->create(key, value)){
		if (transID != -1) 
		log->logCreateSuccess(&memberNode->addr, false, transID, key, value);
		return true;
		}
	if (transID != -1) 
	log->logCreateFail(&memberNode->addr, false, transID, key, value);	
	return false;
		
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key,int transID) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	string content = this->ht->read(key);
	bool success = (content!="");
	if (success) {
		log->logReadSuccess(&memberNode->addr, false, transID, key, content);
	}
	else {
		log->logReadFail(&memberNode->addr, false, transID, key);
	}

	return content;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica,int transID) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	if( this->ht->update(key, value)){
		log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
		return true;
		}
	
	log->logUpdateFail(&memberNode->addr, false, transID, key, value);	
	return false;
		
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key,int transID) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	bool success = this->ht->deleteKey(key);
	
	if (success) {
		log->logDeleteSuccess(&memberNode->addr, false, transID, key);
		return true;
	} 

	log->logDeleteFail(&memberNode->addr, false, transID, key);
	return false;
	
}
void MP2Node::sendreply(string key, MessageType mType, bool success, Address* fromaddr, int transID, string content) {
	MessageType replyType = (mType == MessageType::READ)? MessageType::READREPLY: MessageType::REPLY;
	
	if(replyType == MessageType::READREPLY){
		Message msg(transID, this->memberNode->addr, content);
		string data = msg.toString();
		emulNet->ENsend(&memberNode->addr, fromaddr, data);	
	}else{
		// MessageType::REPLY
		Message msg(transID, this->memberNode->addr, replyType, success);
		string data = msg.toString();
		emulNet->ENsend(&memberNode->addr, fromaddr, data);
	}
	
}
/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */
	
	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg(message);
		/*
		 * Handle the message types here
		 */
		 switch(msg.type){
			case MessageType::CREATE:{
				bool success = createKeyValue(msg.key, msg.value, msg.replica, msg.transID);
				if (msg.transID != -1) {
				sendreply(msg.key, msg.type, success, &msg.fromAddr, msg.transID,"suck");
				}
				break;
			}
			case MessageType::DELETE:{
				bool success = deletekey(msg.key, msg.transID);
				sendreply(msg.key, msg.type, success, &msg.fromAddr, msg.transID,"suck");
				
				break;
			}
			case MessageType::READ:{
				string content = readKey(msg.key, msg.transID);
				bool success = !content.empty();
				sendreply(msg.key, msg.type, success, &msg.fromAddr, msg.transID, content);
				break;
			}
			case MessageType::UPDATE:{
				bool success = updateKeyValue(msg.key, msg.value, msg.replica, msg.transID);
				sendreply(msg.key, msg.type, success, &msg.fromAddr, msg.transID,"suck");
				break;
			}
			case MessageType::READREPLY:{
				map<int, transaction*>::iterator it = transMap.find(msg.transID);
				if(it == transMap.end())
					break;
				transaction* t = transMap[msg.transID];
				t->replyCount ++;
				t->value = msg.value; // content 
				bool success = (msg.value != "");
				
				if(success) {
					t->successCount ++;
				}	
				break;
			}
			case MessageType::REPLY:{
				map<int, transaction*>::iterator it = transMap.find(msg.transID);
				if(it == transMap.end()){
					break;
				}
				
				transaction* t = transMap[msg.transID];
				t->replyCount ++;
				if(msg.success)
					t->successCount ++;
				break;
			}
		}
		/*
		 * Handle the message types here
		 */
		checkTrans();


	}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */

void MP2Node::checkTrans(){
	map<int, transaction*>::iterator it = transMap.begin();
	while (it != transMap.end()){
		if(it->second->replyCount == 3) {
			if(it->second->successCount >= 2) {
				replylog(it->second, true, true, it->first);
			}else{
				replylog(it->second, true, false, it->first); 
			}
			delete it->second;
			it = transMap.erase(it);
			continue;
		}else {
			if(it->second->successCount == 2) {
				replylog(it->second, true, true, it->first);
				
				delete it->second;
				it = transMap.erase(it);
				continue;
			}
			
			if(it->second->replyCount - it->second->successCount == 2) {
				replylog(it->second, true, false, it->first);
				
				delete it->second;
				it = transMap.erase(it);
				continue;
			}
		}
		
		// time limit 
		if(this->par->getcurrtime() - it->second->getTime() > 10) {
				replylog(it->second, true, false, it->first);
				
				delete it->second;
				it = transMap.erase(it);
				continue;
		}

		it++;
	}	
}

void MP2Node::replylog(transaction* t, bool isCoordinator, bool success, int transID) {
	switch (t->mType) {
		case CREATE: {
			if (success) {
				log->logCreateSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			} else {
				log->logCreateFail(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			}
			break;
		}
			
		case READ: {
			if (success) {
				log->logReadSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			} else {
				log->logReadFail(&memberNode->addr, isCoordinator, transID, t->key);
			}
			break;
		}
			
		case UPDATE: {
			if (success) {
				log->logUpdateSuccess(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			} else {
				log->logUpdateFail(&memberNode->addr, isCoordinator, transID, t->key, t->value);
			}
			break;
		}
			
		case DELETE: {
			if (success) {
				log->logDeleteSuccess(&memberNode->addr, isCoordinator, transID, t->key);
			} else {
				log->logDeleteFail(&memberNode->addr, isCoordinator, transID, t->key);
			}
			break;
		}
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	 
	map<string, string>::iterator it;
	for(it = this->ht->hashTable.begin(); it != this->ht->hashTable.end(); it++) {
		string key = it->first;
		string value = it->second;
		stableCreate(key,value);
	}
}
