module Analytics
  # :title:WorkerGroups
  # = Name
  #
  # = Synopsis
  #
  # = Description
  #   Creates files and utilities for Resque Workers. PID's of the workers are
  #   stored as groups in files. Format: @QUEUE.pid.
  # = Todo
  # == Upcoming Features
  # 0. Nothing yet.
  # == Known Issues
  # 0. Nothing yet.
  module WorkerGroups
    @queue = '*' #Default QUEUE if none

    def self.queue(queue)
      @queue = queue
    end

    def Process.exists?(pid)
      kill(0, pid.to_i) rescue false
    end

    def self.pid_directory
      @pid_directory ||= Rails.root.join('tmp', 'pids', "resque")
    end

    def self.pid_directory_for_group
      @pid_directory_for_group ||= Rails.root.join('tmp', 'pids', "resque", @queue)
    end

    def self.group_master_pid
      File.read pid_directory.join("#{@queue}.pid").to_s rescue nil
    end

    def self.group?
      !group_master_pid || group_master_pid.to_s == Process.pid.to_s
    end

    def self.group_running?
      Process.exists?(group_master_pid)
    end
  end
end