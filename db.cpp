#include "ocelot.h"
#include "db.h"
#include "misc_functions.h"
#include <string>
#include <iostream>
#include <queue>
#include <unistd.h>
#include <time.h>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>
#include <boost/lexical_cast.hpp>

#define DB_LOCK_TIMEOUT 50

Mongo::Mongo(std::string mongo_db, std::string mongo_host, std::string username, std::string password) {
   try
   {
      conn.connect("localhost");
   }
   catch(mongo::DBException &e)
   {
      std::cout << "Could not connect " << e.what() << std::endl;
   }

   db = mongo_db, server = mongo_host, db_user = username, pw = password;
   u_active = false; t_active = false; p_active = false; s_active = false; tok_active = false; hist_active = false;

   std::cout << "Connected to mongo" << std::endl;
   update_user_buffer = std::vector<mongo_query>();
   update_torrent_buffer = std::vector<mongo_query>();
   update_peer_buffer = std::vector<mongo_query>();
   update_snatch_buffer = std::vector<mongo_query>();


   logger_ptr = logger::get_instance();
}

void Mongo::load_torrents(std::unordered_map<std::string, torrent> &torrents) {
   mongo::BSONObj fields = BSON("id" << 1 << "info_hash" << 1 << "freetorrent" << 1 << "snatched" << 1);
   std::unique_ptr<mongo::DBClientCursor> cursor = conn.query(db + ".torrents", 
         mongo::Query().sort("id"), 0, 0, &fields);

   mongo::BSONObj doc;
   while(cursor->more())
   {
      doc=cursor->next();
      std::string info_hash = doc.getStringField("info_hash");
      info_hash = hextostr(info_hash);
      std::cout << "Info hash loaded: " << info_hash << std::endl << "error: " << conn.getLastError() << std::endl;
      
      torrent t;
      t.id = doc.getIntField("id");
      t.free_torrent = (doc.getIntField("freetorrent") == 1) ? FREE 
         : (doc.getIntField("freetorrent") == 2) ? NEUTRAL  : NORMAL;
      t.balance = 0;
      t.completed = doc.getIntField("snatched");
      t.last_selected_seeder = "";
      torrents[info_hash] = t;
   }
}

void Mongo::load_users(std::unordered_map<std::string, user> &users) {
   //"SELECT ID, can_leech, torrent_pass FROM users_main WHERE Enabled='1';"
   mongo::BSONObj fields = BSON("id" << 1 << "can_leech" << 1 << "torrent_pass" << 1);
   std::unique_ptr<mongo::DBClientCursor> cursor = conn.query(db + ".users", mongo::Query(BSON("enabled" << 1)), 0, 0, &fields);
   
   mongo::BSONObj doc;
   while(cursor->more())
   {
      doc=cursor->next();
      std::string passkey = doc.getStringField("torrent_pass");

      user u;
      u.id = doc.getIntField("id");
      u.can_leech = doc.getIntField("can_leech");
      users[passkey] = u;
   }
}

void Mongo::load_tokens(std::unordered_map<std::string, torrent> &torrents) {
   //"SELECT uf.UserID, t.info_hash FROM users_freeleeches AS uf JOIN torrents AS t ON t.ID = uf.TorrentID WHERE uf.Expired = '0';"
   //
   mongo::BSONObj fields = BSON("id" << 1 <<  "freeleeches.info_hash" << 1);
   std::unique_ptr<mongo::DBClientCursor> cursor = conn.query(db + ".users", mongo::Query(BSON("freeleeches.expired" << 0)), 0, 0, &fields);
   mongo::BSONObj doc;
   while(cursor->more())
   {
      doc=cursor->next();
      std::string info_hash = doc.getStringField("freeleeches.info_hash");
      std::unordered_map<std::string, torrent>::iterator it = torrents.find(info_hash);
      if (it != torrents.end()) {
         torrent &tor = it->second;
         tor.tokened_users.insert(doc.getIntField("id"));
      }
   }
}


void Mongo::load_whitelist(std::vector<std::string> &whitelist) {
   //"SELECT peer_id FROM xbt_client_whitelist;"
   mongo::BSONObj fields = BSON("peer_id" << 1);
   std::unique_ptr<mongo::DBClientCursor> cursor = conn.query(db + ".torrent_client_whitelist", mongo::Query(), 0, 0, &fields);
   mongo::BSONObj doc;
   while(cursor->more())
   {
      doc = cursor->next();
      whitelist.push_back(doc.getStringField("peer_id"));
   }
}

void Mongo::record_token(int uid, int tid, long long downloaded_change) {
   boost::mutex::scoped_lock lock(user_token_lock);
   mongo_query m;
   m.table = ".users";
   m.query = mongo::Query(BSON("id" << uid << "freeleeches.id" << tid));
   m.data = BSON("$inc" << BSON("downloaded" << downloaded_change));

   update_token_buffer.push_back(m);
}

void Mongo::record_user(int id, long long uploaded_change, long long downloaded_change) {
   boost::mutex::scoped_lock lock(user_buffer_lock);
   mongo_query m;
   m.table = ".users";
   m.query = mongo::Query(BSON("id" << id));
   m.data = BSON("$inc" << BSON("uploaded" << uploaded_change << "downloaded" << downloaded_change));

   update_user_buffer.push_back(m);
}


//TODO: last_action = IF(VALUES(Seeders) > 0, NOW(), last_action);
void Mongo::record_torrent(int tid, int seeders, int leechers, int snatched_change, int balance) {
   boost::mutex::scoped_lock lock(torrent_buffer_lock);
   mongo_query m;
   m.table = ".torrents";
   m.query = mongo::Query(BSON("id" << tid));
   m.data = BSON("$set" << BSON("seeders" << seeders << "leechers" << leechers << "balance" << balance) << "$inc" << BSON("snatched" << snatched_change));

   update_torrent_buffer.push_back(m);
}

/*void record_peer(int uid, int fid, int active, std::string peerid, std::string useragent, std::string &ip, long long uploaded, long long downloaded, long long upspeed, long long downspeed, long long left, time_t timespent, unsigned int announces) { 
   boost::mutex::scoped_lock lock(peer_buffer_lock);
   //q << record << mongopp::quote << ip << ',' << mongopp::quote << peer_id << ',' << mongopp::quote << useragent << "," << time(NULL) << ')';

   update_peer_buffer += q.str();
}*/

// TODO: Double check timespet is correctly upserted and mtime
// Deal with ip, peer_id, useragent better
void Mongo::record_peer(int uid, int fid, int active, std::string peerid, std::string useragent, std::string &ip, long long uploaded, long long downloaded, long long upspeed, long long downspeed, long long left, time_t timespent, unsigned int announces) {
   boost::mutex::scoped_lock (peer_buffer_lock);
   mongo_query m;
   m.table = ".users";
   m.query = mongo::Query(BSON("id" << uid << "files.id" << fid));
   m.data = BSON("$set" << BSON("files.active" << active << "files.uploaded" << uploaded << "files.downloaded" << downloaded << "files.upspeed" << upspeed << "files.downspeed" << downspeed << "files.remaining" << left << "files.timespent" << static_cast<long long>(timespent) << "files.announced" << announces << "files.useragent" << useragent << "files.ip" << ip << "files.peerid" << peerid << "files.mtime" << static_cast<long long>(time(NULL))));
   //q << record << ',' << mongopp::quote << peer_id << ',' << tid << ',' << time(NULL) << ')';
   update_peer_buffer.push_back(m);
}

void Mongo::record_peer_hist(int uid, long long downloaded, long long left, long long uploaded, long long upspeed, long long downspeed, long long tstamp, std::string &peer_id, int tid) {
   boost::mutex::scoped_lock (peer_hist_buffer_lock);
   mongo_query m;
   m.table = ".users";
   m.query = mongo::Query(BSON("id" << uid << "history.tid" << tid));
   m.data = BSON("$set" << BSON("history.downloaded" << downloaded << "history.remaining" << left << "history.uploaded" << uploaded << "history.upspeed" << upspeed << "history.downspeed" << downspeed << "history.timespent" << (tstamp) << "history.peer_id" << peer_id));

   update_peer_hist_buffer.push_back(m);
}

void Mongo::record_snatch(int uid, int tid, time_t tstamp, std::string ip) {
   boost::mutex::scoped_lock lock(snatch_buffer_lock);
   mongo_query m;
   m.table = ".users";
   m.query = mongo::Query(BSON("id" << uid));
   m.data = BSON("snatches.id" << tid << "snatches.tstamp" << static_cast<long long>(tstamp) << "snatches.ip" << ip);

   update_snatch_buffer.push_back(m);
}

bool Mongo::all_clear() {
   return (user_queue.size() == 0 && torrent_queue.size() == 0 && peer_queue.size() == 0 && snatch_queue.size() == 0 && token_queue.size() == 0 && peer_hist_queue.size() == 0);
}

void Mongo::flush() {
   flush_users();
   flush_torrents();
   flush_snatches();
   flush_peers();
   flush_peer_hist();
   flush_tokens();
}

void Mongo::flush_users() {
   boost::mutex::scoped_lock lock(user_buffer_lock);
   if (update_user_buffer.empty()) {
      return;
   }
   user_queue.push(update_user_buffer);
   update_user_buffer.clear();
   if (user_queue.size() == 1 && u_active == false) {
      boost::thread thread(&Mongo::do_flush_users, this);
   }
}

void Mongo::flush_torrents() {
   boost::mutex::scoped_lock lock(torrent_buffer_lock);
   if (update_torrent_buffer.empty()) {
      return;
   }
   torrent_queue.push(update_torrent_buffer);
   update_torrent_buffer.clear();

   if (torrent_queue.size() == 1 && t_active == false) {
      boost::thread thread(&Mongo::do_flush_torrents, this);
   }
}

void Mongo::flush_snatches() {
   boost::mutex::scoped_lock lock(snatch_buffer_lock);
   if (update_snatch_buffer.empty()) {
      return;
   }
   snatch_queue.push(update_snatch_buffer);
   update_snatch_buffer.clear();
   if (snatch_queue.size() == 1 && s_active == false) {
      boost::thread thread(&Mongo::do_flush_snatches, this);
   }
}

void Mongo::flush_peers() {
   boost::mutex::scoped_lock lock(peer_buffer_lock);
   // because xfu inserts are slow and ram is not infinite we need to
   // limit this queue's size
   if (peer_queue.size() >= 1000) {
      peer_queue.pop();
   }
   if (update_peer_buffer.empty()) {
      return;
   }

   /*if (peer_queue.size() == 0) {
      sql = "SET session sql_log_bin = 0";
      peer_queue.push(sql);
      sql.clear();
   }*/

   peer_queue.push(update_peer_buffer);
   update_peer_buffer.clear();
   if (peer_queue.size() == 1 && p_active == false) {
      boost::thread thread(&Mongo::do_flush_peers, this);
   }
}

void Mongo::flush_peer_hist() {
   boost::mutex::scoped_lock lock(peer_hist_buffer_lock);
   if (update_peer_hist_buffer.empty()) {
      return;
   }

   /*if (peer_hist_queue.size() == 0) {
      sql = "SET session sql_log_bin = 0";
      peer_hist_queue.push(sql);
      sql.clear();
   }*/

   peer_hist_queue.push(update_peer_hist_buffer);
   update_peer_hist_buffer.clear();
   if (peer_hist_queue.size() == 1 && hist_active == false) {
      boost::thread thread(&Mongo::do_flush_peer_hist, this);
   }
}

void Mongo::flush_tokens() {
   std::string sql;
   boost::mutex::scoped_lock lock(user_token_lock);
   if (update_token_buffer.empty()) {
      return;
   }
   token_queue.push(update_token_buffer);
   update_token_buffer.clear();
   if (token_queue.size() == 1 && tok_active == false) {
      boost::thread(&Mongo::do_flush_tokens, this);
   }
}

void Mongo::do_flush_users() {
   u_active = true;
   mongo::DBClientConnection c;
   c.connect(server);
   while (user_queue.size() > 0) {
      std::vector<mongo_query> qv = user_queue.front();
      while(!qv.empty())
      {
         mongo_query q = qv.back();
         c.update(db + q.table, q.query, q.data, true);
         if(!c.getLastError().empty())
         {
            std::cout << "User flush error: " << c.getLastError() << ". " << user_queue.size() << " don't remain." << std::endl;
            sleep(3);
            continue;
         }
         else
         {
            qv.pop_back();
            std::cout << "Users flushed (" << user_queue.size() << " remain)" << std::endl;
         }
      }
      boost::mutex::scoped_lock lock(user_buffer_lock);
      user_queue.pop();
   } 
   u_active = false;
}

void Mongo::do_flush_torrents() {
   t_active = true;
   mongo::DBClientConnection c;
   c.connect(server);
   while (torrent_queue.size() > 0) {
      std::vector<mongo_query> qv = torrent_queue.front();
      while(!qv.empty())
      {
         mongo_query q = qv.back();
         c.update(db + q.table, q.query, q.data, true);
         
         if(!c.getLastError().empty())
         {
            std::cout << "Torrent flush error: " << c.getLastError() << ". " << torrent_queue.size() << " don't remain." << std::endl;
            sleep(3);
            continue;
         }
         else
         {
            qv.pop_back();
            std::cout << "Torrents flushed (" << torrent_queue.size() << " remain)" << std::endl;
         }
      }
      boost::mutex::scoped_lock lock(torrent_buffer_lock);
      torrent_queue.pop();
   }
   t_active = false;
}

void Mongo::do_flush_peers() {
   p_active = true;
   mongo::DBClientConnection c;
   c.connect(server);
   while (peer_queue.size() > 0) {
      std::vector<mongo_query> qv = peer_queue.front();
      while(!qv.empty())
      {
         mongo_query q = qv.back();
         c.update(db + q.table, q.query, q.data, true);
         
         if(!c.getLastError().empty())
         {
            std::cout << "Peer flush error: " << c.getLastError() << ". " << peer_queue.size() << " don't remain." << std::endl;
            sleep(3);
            continue;
         }
         else
         {
            qv.pop_back();
            std::cout << "Peer flushed (" << peer_queue.size() << " remain)" << std::endl;
         }
      }
      boost::mutex::scoped_lock lock(peer_buffer_lock);
      peer_queue.pop();
   }
   p_active = false;
}

void Mongo::do_flush_peer_hist() {
   hist_active = true;
   mongo::DBClientConnection c;
   c.connect(server);
   while (peer_hist_queue.size() > 0) {
      std::vector<mongo_query> qv = peer_hist_queue.front();
      while(!qv.empty())
      {
         mongo_query q = qv.back();
         c.update(db + q.table, q.query, q.data, true);
         
         if(!c.getLastError().empty())
         {
            std::cout << "Peer hist flush error: " << c.getLastError() << ". " << peer_hist_queue.size() << " don't remain." << std::endl;
            sleep(3);
            continue;
         }
         else
         {
            qv.pop_back();
            std::cout << "Peer hist flushed (" << peer_hist_queue.size() << " remain)" << std::endl;
         }
      }
      boost::mutex::scoped_lock lock(peer_hist_buffer_lock);
      peer_hist_queue.pop();
   }
   hist_active = false;
}

void Mongo::do_flush_snatches() {
   s_active = true;
   mongo::DBClientConnection c;
   c.connect(server);
   while (snatch_queue.size() > 0) {
      std::vector<mongo_query> qv = snatch_queue.front();
      while(!qv.empty())
      {
         mongo_query q = qv.back();
         c.update(db + q.table, q.query, q.data, true);
         
         if(!c.getLastError().empty())
         {
            std::cout << "Snatch flush error: " << c.getLastError() << ". " << snatch_queue.size() << " don't remain." << std::endl;
            sleep(3);
            continue;
         }
         else
         {
            qv.pop_back();
            std::cout << "Snatch flushed (" << snatch_queue.size() << " remain)" << std::endl;
         }
      }
      boost::mutex::scoped_lock lock(snatch_buffer_lock);
      snatch_queue.pop();
   }
   s_active = false;
}

void Mongo::do_flush_tokens() {
   tok_active = true;
   mongo::DBClientConnection c;
   c.connect(server);
   while (token_queue.size() > 0) {
      std::vector<mongo_query> qv = token_queue.front();
      while(!qv.empty())
      {
         mongo_query q = qv.back();
         c.update(db + q.table, q.query, q.data, true);
         
         if(!c.getLastError().empty())
         {
            std::cout << "Token flush error: " << c.getLastError() << ". " << token_queue.size() << " don't remain." << std::endl;
            sleep(3);
            continue;
         }
         else
         {
            qv.pop_back();
            std::cout << "Token flushed (" << token_queue.size() << " remain)" << std::endl;
         }
      }
      boost::mutex::scoped_lock lock(user_token_lock);
      token_queue.pop();
   }
   tok_active = false;
}
