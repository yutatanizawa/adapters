require "time"
require 'etc'
require "ood_core/refinements/hash_extensions"
require "ood_core/refinements/array_extensions"
require "ood_core/job/adapters/helper"

require 'json'
require 'pathname'

module OodCore
  module Job
		class Factory

      using Refinements::HashExtensions
			# Build the PSIJ adapter from a configuration
      # @param config [#to_h] the configuration for job adapter
      # @option config [Object] :bin (nil) Path to PSIJ binaries
      # @option config [#to_h]  :bin_overrides ({}) Optional overrides to PSIJ executables
			def self.build_psij(config)
        c = config.to_h.symbolize_keys
        cluster              = c.fetch(:cluster, nil)
        conf                 = c.fetch(:conf, nil)
        bin                  = c.fetch(:bin, nil)
        bin_overrides        = c.fetch(:bin_overrides, {})
        submit_host          = c.fetch(:submit_host, "")
        strict_host_checking = c.fetch(:strict_host_checking, false)
        executor             = c.fetch(:executor, nil)
        queue_name           = c.fetch(:queue_name, nil)
        psij = Adapters::PSIJ::Batch.new(cluster: cluster, conf: conf, bin: bin, bin_overrides: bin_overrides, submit_host: submit_host, strict_host_checking: strict_host_checking, executor: executor, queue_name: queue_name)
        Adapters::PSIJ.new(psij: psij)
      end
		end

		module Adapters
      class PSIJ < Adapter
        class Batch

          attr_reader :cluster
          attr_reader :conf
          attr_reader :bin
          attr_reader :bin_overrides
          attr_reader :submit_host
          attr_reader :strict_host_checking
          attr_reader :executor
          attr_reader :queue_name

          class Error < StandardError; end

          def initialize(cluster: nil, bin: nil, conf: nil, bin_overrides: {}, submit_host: "", strict_host_checking: false, executor: nil, queue_name: nil)
            @cluster              = cluster && cluster.to_s
            @conf                 = conf    && Pathname.new(conf.to_s)
            @bin                  = Pathname.new(bin.to_s)
            @bin_overrides        = bin_overrides
            @submit_host          = submit_host.to_s
            @strict_host_checking = strict_host_checking
            @executor             = executor
            @queue_name           = queue_name
          end

          def get_jobs(id: "")
            id = id.to_s.strip()
            params = {
              id: id,
              executor: executor,
            }
            args = params.map { |k, v| "--#{k}=#{v}" }
            get_info_path = Pathname.new(__FILE__).dirname.expand_path.join("psij/get_info.py").to_s
            jobs_data = call("python3", get_info_path, *args)
            jobs_data = JSON.parse(jobs_data)
            jobs_data
          end

          def submit_job_path(args: [], chdir: nil)
            submit_path = Pathname.new(__FILE__).dirname.expand_path.join("psij/submit.py").to_s
            call("python3", submit_path, *args, chdir: chdir)
          end

          def delete_job(args: [])
            delete_path = Pathname.new(__FILE__).dirname.expand_path.join("psij/delete.py").to_s
            call("python3", delete_path, *args)
          rescue => e
            raise JobAdapterError, e
          end

          def hold_job(args: [])
            hold_path = Pathname.new(__FILE__).dirname.expand_path.join("psij/hold.py").to_s
            call("python3", hold_path, *args)
          end

          def release_job(args: [])
            release_path = Pathname.new(__FILE__).dirname.expand_path.join("psij/release.py").to_s
            call("python3", release_path, *args)
          end

          private
            # Call a forked psij script for a given cluster
            def call(cmd, *args, env: {}, stdin: "", chdir: nil)
              cmd = cmd.to_s
              cmd, args = OodCore::Job::Adapters::Helper.ssh_wrap(submit_host, cmd, args, strict_host_checking=false)
              chdir ||= "."
              o, e, s = Open3.capture3(env, cmd, *(args.map(&:to_s)), stdin_data: stdin.to_s, chdir: chdir.to_s)
              s.success? ? o : raise(Error, e)
            end

        end
        

        STATE_MAP = {
          'Assigned'      => :queued,
          'Chkpnting'     => :queued,
          'Exclude'       => :queued,
          'Exited'        => :completed,
          'Exiting'       => :completed,
          'Held'          => :queued_held,
          'Post-running'  => :completed,
          'Pre-running'   => :queued,
          'Queued'        => :queued,
          'Resuming'      => :queued,
          'Running'       => :running,
          'Staged'        => :queued,
          'Staging'       => :queued,
          'Suspending'    => :suspended,
          'Suspended'     => :suspended,
          'Wating'        => :queued,
        }

        def initialize(opts = {})
          o = opts.to_h.symbolize_keys
        
          @psij = o.fetch(:psij) { raise ArgumentError, "No psij object specified. Missing argument: psij" }
        end
        

        # The `submit` method saves a job script as a file and prepares a command to submit the job.
        # Each optional argument specifies job dependencies (after, afterok, afternotok, afterany).
        def submit(script, after: [], afterok: [], afternotok: [], afterany: [])

          content = if script.shell_path.nil?
            script.content
          else
            "#!#{script.shell_path}\n#{script.content}"
          end

          native = script.native.join("\n")
          script.content.concat(native)

          duration = script.content.match(/elapstim_req\s*=\s*(\d{1,2}(:\d{1,2})?(:\d{1,2})?)/)
          duration = duration ? duration[1] : nil
          queue_name = script.content.match(/#PBS\s*-q\s*(\S+)/)
          queue_name = queue_name ? queue_name[1] : nil
          project_name = script.content.match(/#PBS\s*-A\s*(\S+)/)
          project_name = project_name ? project_name[1] : nil
          reservation_id = script.content.match(/#PBS\s*-y\s*(\S+)/)
          reservation_id = reservation_id ? reservation_id[1] : nil

          exclusive_node_use = script.content.match(/#PBS\s*--exclusive/) ? true : false
          node_count = script.content.match(/#PBS\s*-b\s*(\S+)/)
          node_count = node_count ? node_count[1] : nil
          memory_per_node = script.content.match(/#PBS\s*--memsz-lhost\s*=\s*(\S+)/)
          memory_per_node = memory_per_node ? memory_per_node[1] : nil
          gpu_cores_per_process = script.content.match(/#PBS\s*--gpunum-lhost\s*=\s*(\S+)/)
          gpu_cores_per_process = gpu_cores_per_process ? gpu_cores_per_process[1] : nil

          stdout_path = script.output_path.nil? ? File.join(Dir.pwd, "stdout.txt") : script.output_path
          stderr_path = script.error_path.nil? ? File.join(File.dirname(stdout_path), "stderr.txt") : script.error_path

          relative_path = "~/ood_tmp/run.sh"

          full_path = File.expand_path("~/ood_tmp/run.sh")

          FileUtils.mkdir_p(File.dirname(full_path))

          File.open(full_path, "w") do |file|
            file.write(content)
          end

          File.chmod(0755, full_path)
          
          params = {
            job_path: relative_path,
            executor: @psij.executor,
            stdout_path: stdout_path.to_s,
            stderr_path: stderr_path.to_s,
            duration: duration,
            queue_name: @psij.queue_name,
            project_name: project_name,
            reservation_id: reservation_id,
            exclusive_node_use: exclusive_node_use,
            node_count: node_count.to_i,
            memory_per_node: memory_per_node.to_i,
            gpu_cores_per_process: gpu_cores_per_process.to_i
          }

          args = params.map { |k, v| "--#{k}=#{v}" }

          @psij.submit_job_path(args: args, chdir: script.workdir)
        rescue Batch::Error => e
          raise JobAdapterError, e
        end

        def cluster_info
        end

        def accounts
        end
        
        def delete(id)
          id = id.to_s.strip()
          params = {
            id: id,
            executor: @psij.executor,
          }
          args = params.map { |k, v| "--#{k}=#{v}" }
          @psij.delete_job(args: args)
        end

        def hold(id)
          id = id.to_s.strip()
          params = {
            id: id,
            executor: @psij.executor,
          }
          args = params.map { |k, v| "--#{k}=#{v}" }
          @psij.hold_job(args: args)
        rescue Batch::Error => e
          raise JobAdapterError, e.message unless /Invalid job id specified/ =~ e.message
        end

        def release(id)
          id = id.to_s.strip()
          params = {
            id: id,
            executor: @psij.executor,
          }
          args = params.map { |k, v| "--#{k}=#{v}" }
          @psij.release_job(args: args)
        rescue Batch::Error => e
          raise JobAdapterError, e.message unless /Invalid job id specified/ =~ e.message
        end


        def info(id)
          id = id.to_s

          job_infos = @psij.get_jobs(id: id).map do |v|
            parse_job_info(v)
          end

          if job_infos.empty?
            Info.new(id: id, status: :completed)
          else
            job_infos.first
          end
        end

        def info_all(attrs: nil)
          @psij.get_jobs.map do |v|
            parse_job_info(v)
          end
        end

        def info_where_owner(owner, attrs: nil)
          owner = Array.wrap(owner).map(&:to_s).join(',')
          @slurm.get_jobs.map do |v|
            parse_job_info(v)
          end
        rescue Batch::Error => e
          raise JobAdapterError, e.message
        end

        def status(id)
          info(id.to_s).status
        end

        def directive_prefix
        end

        private
          def get_state(st)
            STATE_MAP.fetch(st, :undetermined)
          end

          def parse_job_info(v)
            # 入力されたHashをInfoクラスにパースする
            allocated_nodes = [ { name: nil } ] * v.fetch(:Resource_List, {})[:nodect].to_i
            Info.new(
              id: v["native_id"],
              status: get_state(v["current_state"]),
              allocated_nodes: allocated_nodes,
              submit_host: nil,
              job_name: v["job_name"],
              job_owner: v["job_owner"],
              accounting_id: v["group_name"],
              procs: v["procs"].to_i,
              queue_name: v["queue_name"],
              wallclock_time: v["wall_time"],
              wallclock_limit: v["wallclock_limit"],
              cpu_time: v["cpu_time"].to_i,
              submission_time: v["submission_time"],
              dispatch_time: v["dispatch_time"],
              native: v
            )
          end

      end
    end
  end
end