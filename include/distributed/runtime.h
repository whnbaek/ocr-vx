/*
 * This file is subject to the license agreement located in the file 
 * LICENSE_UNIVIE and cannot be distributed without it. This notice
 * cannot be removed or modified.
 */

#ifndef OCR_TBB_distributed__runtime_H_GUARD
#define OCR_TBB_distributed__runtime_H_GUARD

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <atomic>
#include <vector>
#include <tbb/task_group.h>

namespace ocr_tbb
{
	namespace distributed
	{
		struct edt; // forward declaration for paused_task_entry

		// Emulates the old empty "barrier task" whose reference count gated completion:
		// each outstanding reference is a deferred task_handle holding task_group::wait()
		// open until run or destroyed. Awaited cooperatively via task_group::wait().
		struct task_barrier {
			task_barrier(tbb::task_group& tg, int initial) : cursor_(initial) {
				sentinels_.reserve(initial);
				for (int i = 0; i < initial; ++i) sentinels_.push_back(tg.defer([]{}));
			}
			void decrement() {
				int idx = --cursor_;
				sentinels_[idx] = tbb::task_handle{};
			}
		private:
			std::vector<tbb::task_handle> sentinels_;
			std::atomic<int> cursor_;
		};


		struct binary_file_writer
		{
			binary_file_writer(thread_context* ctx, const char* name) : ctx(ctx)
			{
				f = fopen(name, "wb");
			}
			~binary_file_writer()
			{
				fclose(f);
			}
			void write(const char* name, const void* buf, std::size_t size)
			{
				fwrite(buf, 1, size, f);
			}
			template<typename T>
			void write_obj(const char* name, const T& x)
			{
				x.write(*this);
			}
			template<typename T>
			void write_ref(const char* name, const T& x)
			{
				fwrite(&x, sizeof(T), 1, f);
			}
			template<typename IT>
			void write_vals(const char* name, IT begin, IT end)
			{
				write_val<u64>(name, "_size", std::distance(begin, end));
				for (IT it = begin; it != end; ++it)
				{
					write_val(name, *it);
				}
			}
			template<typename IT>
			void write_objs(const char* name, IT begin, IT end)
			{
				write_val<u64>(name, "_size", std::distance(begin, end));
				for (IT it = begin; it != end; ++it)
				{
					write_obj(name, *it);
				}
			}
			template<typename T>
			void write_val(const char* name, T x)
			{
				fwrite(&x, sizeof(T), 1, f);
			}
			template<typename T>
			void write_val(const char* name, const char* name_postfix, T x)
			{
				fwrite(&x, sizeof(T), 1, f);
			}
			template<typename T>
			void write_val(const char* name, T x, const char* value_as_text)
			{
				fwrite(&x, sizeof(T), 1, f);
			}
		private:
			FILE* f;
		public:
			thread_context* ctx;
		};

		struct binary_file_reader
		{
			binary_file_reader(thread_context* ctx, const char* name) : ctx(ctx)
			{
				f = fopen(name, "rb");
			}
			~binary_file_reader()
			{
				fclose(f);
			}
			void read(void* buf, std::size_t size)
			{
				fread(buf, 1, size, f);
			}
			template<typename T>
			T read_val()
			{
				T res;
				fread(&res, sizeof(T), 1, f);
				return res;
			}
			template<typename T>
			void read_atomic(std::atomic<T>& x)
			{
				x = read_val<T>();
			}
			template<typename T>
			T read_val(T*)
			{
				T res;
				fread(&res, sizeof(T), 1, f);
				return res;
			}
			template<typename T>
			void read_ref(T& x)
			{
				fread(&x, sizeof(T), 1, f);
			}
			template<typename IT>
			void read_objs(IT it)
			{
				std::size_t count = read_val<u64>();
				while (count--)
				{
					
					*it++ = typename IT::container_type::value_type(*this, read_tag());
				}
			}
			template<typename IT>
			void read_vals(IT it)
			{
				std::size_t count = read_val<u64>();
				while (count--)
				{

					*it++ = read_val<typename IT::container_type::value_type>();
				}
			}
		private:
			FILE* f;
		public:
			thread_context* ctx;
		};

		struct runtime
		{
#if (SIMULATE_MULTIPLE_NODES)
			static task_barrier* get_barrier() { return runtimes_[0]->barrier_; }
			static void set_barrier(task_barrier* b) { runtimes_[0]->barrier_ = b; }
			static tbb::task_group* get_task_group() { return runtimes_[0]->tg_; }
			static void set_task_group(tbb::task_group* tg) { runtimes_[0]->tg_ = tg; }
			static void shutdown(thread_context* ctx)
			{
				the(ctx).was_shut_down_ = true;
				//if (compute_node::get_my_id() == 0) -- all "processes" take part in the shutdown, to make sure they are all ready to terminate
				{
					get_barrier()->decrement();
				}
			}
#else
			static task_barrier* get_barrier() { return the().barrier_; }
			static void set_barrier(task_barrier* b) { the().barrier_ = b; }
			static tbb::task_group* get_task_group() { return the().tg_; }
			static void set_task_group(tbb::task_group* tg) { the().tg_ = tg; }
			static void shutdown(thread_context* ctx)
			{
				get_barrier()->decrement();
			}
#endif
			static edt* get_current_task(thread_context* ctx) { return ctx->current_edt; }
			static void set_current_task(thread_context* ctx, edt* t) { ctx->current_edt = t; }
#if (SIMULATE_MULTIPLE_NODES)
			static void initialize_nodes(std::size_t count, communicator_base* comm)
			{
				for (std::size_t i = 0; i < count; ++i)
				{
					runtimes_.push_back(std::shared_ptr<runtime>(new runtime((node_id)i, comm, count)));
				}
			}
#else
			static void initialize(node_id id, std::size_t count, communicator_base* comm)
			{
				the_ = std::unique_ptr<runtime>(new runtime(id, comm, count));
			}
			static void finalize()
			{
				the_->the_communicator_ = 0;
			}
#endif
			static void global_pause(thread_context* ctx)
			{
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": init global pause");
				the(ctx).nodes_to_pause_ = communicator::number_of_nodes();
				for (node_id i = 0; i < communicator::number_of_nodes(); ++i)
				{
					communicator::send::CMD_pause(ctx, i);
				}
				while (the(ctx).nodes_to_pause_.load()) std::this_thread::yield();
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": finished global pause");
				the(ctx).nodes_to_pause_ = communicator::number_of_nodes();
				for (node_id i = 0; i < communicator::number_of_nodes(); ++i)
				{
					communicator::send::CMD_start_flush(ctx, i);
				}
				while (the(ctx).nodes_to_pause_.load()) std::this_thread::yield();
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": finished global flush");
			}
			static void global_resume(thread_context* ctx)
			{
				for (node_id i = 0; i < communicator::number_of_nodes(); ++i)
				{
					communicator::send::CMD_resume(ctx, i);
				}
			}
			static void notify_paused(thread_context* ctx, node_id node)
			{
				--the(ctx).nodes_to_pause_;
			}
			static void notify_flushed(thread_context* ctx, node_id node)
			{
				--the(ctx).nodes_to_pause_;
			}
			static void pause(thread_context* ctx)
			{
				the(ctx).is_paused_ = true;
				while (runtime_state_observer::running_task_count(ctx) > 0) std::this_thread::yield();
				//all running tasks are done now, no new tasks will be started, since setting is_paused_ forces them to be added to the paused_tasks_ list instead
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": paused");
			}
			static void flush(thread_context* ctx, node_id command_origin)
			{
				communicator::send::CMD_flush(ctx, command_origin);
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": flushed");
			}
			static bool is_paused(thread_context* ctx)
			{
				return the(ctx).is_paused_;
			}
			
			static void add_paused_task(thread_context* ctx, edt* task, tbb::task_handle handle)
			{
				//tbb::spin_mutex::scoped_lock lock(the().paused_tasks_mutex_);
				the(ctx).paused_tasks_.push_back({task, std::move(handle)});
			}
			static void add_paused_message(thread_context* ctx, command_processor::message* m)
			{
				the(ctx).paused_messages_.push_back(m);
			}
			static void global_save(thread_context* ctx)
			{
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": init global save");
				the(ctx).nodes_to_pause_ = communicator::number_of_nodes();
				for (node_id i = 0; i < communicator::number_of_nodes(); ++i)
				{
					communicator::send::CMD_save(ctx, i);
				}
				while (the(ctx).nodes_to_pause_.load()) std::this_thread::yield();
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": finished global save");
			}
			static void notify_saved(thread_context* ctx, node_id node)
			{
				--the(ctx).nodes_to_pause_;
			}
			static void save(thread_context* ctx);
			static void global_load(thread_context* ctx)
			{
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": init global load");
				the(ctx).nodes_to_pause_ = communicator::number_of_nodes();
				for (node_id i = 0; i < communicator::number_of_nodes(); ++i)
				{
					communicator::send::CMD_load(ctx, i);
				}
				while (the(ctx).nodes_to_pause_.load()) std::this_thread::yield();
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": finished global load");
			}
			static void notify_loaded(thread_context* ctx, node_id node)
			{
				--the(ctx).nodes_to_pause_;
			}
			static void load(thread_context* ctx)
			{
				assert(is_paused(ctx));
				ocr_tbb::distributed::binary_file_reader br(ctx,("p" + std::to_string(ocr_tbb::distributed::compute_node::get_my_id(ctx)) + ".bdump").c_str());
				ocr_tbb::distributed::runtime::clear(ctx);
				ocr_tbb::distributed::runtime::read(br);
			}
			static void resume(thread_context* ctx)
			{
				tbb::spin_mutex::scoped_lock lock(the(ctx).paused_tasks_mutex_);//the lock should only be necessary in the node faking mode
				the(ctx).is_paused_ = false;
				for (paused_tasks_type::iterator it = the(ctx).paused_tasks_.begin(); it != the(ctx).paused_tasks_.end(); ++it)
				{
					runtime_state_observer::increment_running_task_count(ctx);
					get_task_group()->run(std::move(it->handle));
				}
				the(ctx).paused_tasks_.clear();
				for (paused_messages_type::iterator it = the(ctx).paused_messages_.begin(); it != the(ctx).paused_messages_.end(); ++it)
				{
					std::unique_ptr<command_processor::message> pm(*it);
					command_processor::process_command(ctx, pm->main.cmd, pm);
				}
				the(ctx).paused_messages_.clear();
				LOCKED_COUT(compute_node::get_my_id(ctx) << ": resumed");
			}
			static void barrier(thread_context* ctx, node_id root)
			{
				the(ctx).barrier_done_ = false;
				communicator::send::CMD_barrier(ctx, root);
				while (the(ctx).barrier_done_.load() == false) std::this_thread::yield();
			}
			static void notify_barrier(thread_context* ctx)
			{
				// Monotonic arrival counter: every N-th CMD_barrier completes one
				// round.  A countdown-with-reset has a window between hitting 0 and
				// the reset in which an already-released rank's next-round
				// CMD_barrier is lost, leaving the next round permanently one short
				// (count stuck above 0, release never broadcast).  Counting up and
				// releasing on each multiple of N has no reset to race against.
				if ((++the(ctx).barrier_count_) % communicator::number_of_nodes() == 0)
				{
					for (node_id i = 0; i < communicator::number_of_nodes(); ++i)
					{
						communicator::send::CMD_barrier_done(ctx, i);
					}
				}
			}
			static void notify_barrier_done(thread_context* ctx)
			{
				the(ctx).barrier_done_ = true;
			}
			static bool is_running(thread_context* ctx)
			{
				if (the(ctx).is_paused_) return false;
				return !the(ctx).was_shut_down_;
			}
			static u64 get_next_map_id(thread_context* ctx)
			{
				return ++the(ctx).map_sequence_id_;
			}
			static void log_message(thread_context* ctx, const command_processor::message& m)
			{
				++the(ctx).message_counts_[m.main.to].the[m.main.cmd];
				the(ctx).message_sizes_[m.main.to].the[m.main.cmd] += m.get_size() + m.followup_size();
			}
			static void save_message_log(thread_context* ctx)
			{
#if (SIMULATE_MULTIPLE_NODES)
				for (std::size_t n = 0; n < runtimes_.size(); ++n)
				{
					runtimes_[n]->save_message_log_impl((node_id)n);
				}
#else
				the_->save_message_log_impl(compute_node::get_my_id(ctx));
#endif
			}
			void save_message_log_impl(node_id node)
			{
				std::string file_name = "log/msg" + std::to_string(node) + ".csv";
				FILE* f = fopen(file_name.c_str(), "w");
				if (f)
				{
					for (tbb::concurrent_unordered_map<node_id, message_count_type>::const_iterator it = message_counts_.begin(); it != message_counts_.end(); ++it)
					{
						std::size_t total_count = 0;
						std::size_t total_size = 0;
						for (tbb::concurrent_unordered_map<command, std::atomic<std::size_t> >::const_iterator cit = it->second.the.begin(); cit != it->second.the.end(); ++cit)
						{
							command cmd = cit->first;
							total_count += cit->second.load();
							total_size += message_sizes_[it->first].the[cit->first].load();
							fprintf(f, "%d,%d,%s,%d,%d\n", (int)node, (int)it->first, command_processor::describe(cmd).name.c_str(), (int)cit->second.load(), (int)message_sizes_[it->first].the[cit->first].load());
						}
						//fprintf(f, "%d,%d,total,%d,%d\n", (int)node, (int)it->first, (int)total_count, (int)total_size);
					}
					fclose(f);
				}
			}
#ifdef ENABLE_EXTENSION_HETEROGENEOUS_FUNCTIONS
			static void register_edt_function(thread_context* ctx, ocrEdt_t func_ptr)
			{
				the(ctx).edt_functions_.push_back(func_ptr);
			}
			static ocrEdt_t edt_function_index_to_ptr(thread_context* ctx, u64 index)
			{
				return the(ctx).edt_functions_[index];
			}
			static u64 edt_function_ptr_to_index(thread_context* ctx, ocrEdt_t func_ptr)
			{
				for (std::size_t i = 0; i < the(ctx).edt_functions_.size(); ++i)
				{
					if (the(ctx).edt_functions_[i] == func_ptr) return (u64)i;
				}
				assert(0);
				return -1;
			}
			static void register_user_function(thread_context* ctx, ocrFuncPtr_t func_ptr)
			{
				the(ctx).user_functions_.push_back(func_ptr);
			}
			static ocrFuncPtr_t user_function_index_to_ptr(thread_context* ctx, u64 index)
			{
				return the(ctx).user_functions_[index];
			}
			static u64 user_function_ptr_to_index(thread_context* ctx, ocrFuncPtr_t func_ptr)
			{
				for (std::size_t i = 0; i < the(ctx).user_functions_.size(); ++i)
				{
					if (the(ctx).user_functions_[i] == func_ptr) return (u64)i;
				}
				assert(0);
				return -1;
			}
#endif
			runtime(node_id id, communicator_base* comm, std::size_t number_of_nodes)
				: the_compute_node_(id),
				the_object_cache_(number_of_nodes),
				the_communicator_(comm),
				barrier_(0),
				tg_(nullptr),
				is_paused_(false),
				was_shut_down_(false)
			{
				map_sequence_id_ = 1;
				barrier_count_ = 0;   // monotonic arrival counter (see notify_barrier)
			}
			template<typename Writer>
			static void write(Writer& w);

			static void clear(thread_context* ctx)
			{
				{
					tbb::spin_mutex::scoped_lock lock(the(ctx).paused_tasks_mutex_);//the lock should only be necessary in the node faking mode
					the(ctx).paused_messages_.clear();
					// Paused tasks are discarded without execution. Destroying each deferred
					// task_handle releases the wait reference it was holding, so clearing the
					// container alone restores the barrier's reference count.
					the(ctx).paused_tasks_.clear();
				}
				the(ctx).the_command_processor_.clear(ctx);
				the(ctx).the_compute_node_.clear(ctx);
				the(ctx).the_communicator_->clear(ctx);
				the(ctx).the_object_repository_.clear(ctx);
				the(ctx).the_object_cache_.clear(ctx);
				the(ctx).map_sequence_id_ = 1;
#ifdef ENABLE_EXTENSION_HETEROGENEOUS_FUNCTIONS
				the(ctx).edt_functions_.clear();
				the(ctx).user_functions_.clear();
#endif
			}

			template<typename Reader>
			static void read(Reader& r);
		public:
			command_processor the_command_processor_;
			compute_node the_compute_node_;
			object_repository the_object_repository_;
			object_cache the_object_cache_;
			communicator_base* the_communicator_;
			runtime_state_observer the_state_observer_;
		private:
			task_barrier* barrier_;
			tbb::task_group* tg_;
			bool is_paused_;
			bool was_shut_down_;
			tbb::spin_mutex paused_tasks_mutex_;
			struct paused_task_entry { edt* task; tbb::task_handle handle; };
			typedef tbb::concurrent_vector<paused_task_entry> paused_tasks_type;
			paused_tasks_type paused_tasks_;
			typedef tbb::concurrent_vector<command_processor::message*> paused_messages_type;
			paused_messages_type paused_messages_;
			std::atomic<std::size_t> nodes_to_pause_;
			std::atomic<largest_atomic_int_t> map_sequence_id_;
			std::atomic<bool> barrier_done_;
			std::atomic<largest_atomic_int_t> barrier_count_;
			//the following "encapsulation" is done only to keep visual studio from complaining about the name being too long
			struct message_count_type
			{
				tbb::concurrent_unordered_map<command, std::atomic<std::size_t> > the;
			};
			tbb::concurrent_unordered_map<node_id, message_count_type> message_counts_;
			struct message_size_type
			{
				tbb::concurrent_unordered_map<command, std::atomic<std::size_t> > the;
			};
			tbb::concurrent_unordered_map<node_id, message_size_type> message_sizes_;
#ifdef ENABLE_EXTENSION_HETEROGENEOUS_FUNCTIONS
			std::vector<ocrEdt_t> edt_functions_;
			std::vector<ocrFuncPtr_t> user_functions_;
#endif

#if (SIMULATE_MULTIPLE_NODES)
			static std::vector<std::shared_ptr<runtime> > runtimes_;
#else
			static std::unique_ptr<runtime> the_;
#endif
		public:
			static communicator_base* the_communicator()
			{
#if (SIMULATE_MULTIPLE_NODES)
				return runtimes_[0]->the_communicator_;
#else
				return the_->the_communicator_;
#endif
			}
#if (!SIMULATE_MULTIPLE_NODES)
			static runtime& the()
			{
				return *the_;
			}
#endif
			static runtime& the(thread_context* ctx)
			{
#if (SIMULATE_MULTIPLE_NODES)
				return *runtimes_[(std::size_t)ctx->node];
#else
				return *the_;
#endif
			}
			friend struct observer;
		};
	}
}

#endif
