#ifndef OCELOT_DB_H
#define OCELOT_DB_H
#pragma GCC visibility push(default)
#include <mongo/client/dbclient.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <queue>
#include <boost/thread/mutex.hpp>

#include "logger.h"

class Mongo {
	private:

                struct mongo_query {
                   std::string table;
                   mongo::Query query;
                   mongo::BSONObj data;
                };


		mongo::DBClientConnection conn;
                std::vector<mongo_query> update_user_buffer;
		std::vector<mongo_query> update_torrent_buffer;
		std::vector<mongo_query> update_peer_buffer;
		std::vector<mongo_query> update_snatch_buffer;
		std::vector<mongo_query> update_token_buffer;
		std::vector<mongo_query> update_peer_hist_buffer;
		
		std::queue<std::vector<mongo_query>> user_queue;
		std::queue<std::vector<mongo_query>> torrent_queue;
		std::queue<std::vector<mongo_query>> peer_queue;
		std::queue<std::vector<mongo_query>> snatch_queue;
		std::queue<std::vector<mongo_query>> token_queue;
		std::queue<std::vector<mongo_query>> peer_hist_queue;

		std::string db, server, db_user, pw;
		bool u_active, t_active, p_active, s_active, tok_active, hist_active;


		// These locks prevent more than one thread from reading/writing the buffers.
		// These should be held for the minimum time possible.
		boost::mutex user_buffer_lock;
		boost::mutex torrent_buffer_lock;
		boost::mutex peer_buffer_lock;
		boost::mutex snatch_buffer_lock;
		boost::mutex user_token_lock;
		boost::mutex peer_hist_buffer_lock;
		
		void do_flush_users();
		void do_flush_torrents();
		void do_flush_snatches();
		void do_flush_peers();
		void do_flush_tokens();
		void do_flush_peer_hist();

		void flush_users();
		void flush_torrents();
		void flush_snatches();
		void flush_peers();
		void flush_tokens();
		void flush_peer_hist();

	public:
		Mongo(std::string mongo_db, std::string mongo_host, std::string username, std::string password);
		void load_torrents(std::unordered_map<std::string, torrent> &torrents);
		void load_users(std::unordered_map<std::string, user> &users);
		void load_tokens(std::unordered_map<std::string, torrent> &torrents);
		void load_whitelist(std::vector<std::string> &whitelist);
		
		void record_user(int id, long long uploaded_change, long long downloaded_change); // (id,uploaded_change,downloaded_change)
		void record_torrent(int tid, int seeders, int leechers, int snatched_change, int balance); // (id,seeders,leechers,snatched_change,balance)
		void record_snatch(int uid, int tid, time_t tstamp, std::string ip); // (uid,fid,tstamp,ip)
		void record_peer(int uid, int fid, int active, std::string peerid, std::string useragent, std::string &ip, long long uploaded, long long downloaded, long long upspeed, long long downspeed, long long left, time_t timespent, unsigned int announces);  ; // (uid,fid,active,peerid,useragent,ip,uploaded,downloaded,upspeed,downspeed,left,timespent,announces)
		void record_token(int uid, int tid, long long downloaded_change);
		void record_peer_hist(int uid, long long downloaded, long long left, long long uploaded, long long upspeed, long long downspeed, long long tstamp, std::string &peer_id, int tid);

		void flush();

		bool all_clear();
		
		boost::mutex torrent_list_mutex;

		logger* logger_ptr;
};

#pragma GCC visibility pop
#endif
