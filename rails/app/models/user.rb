# :title:User
# = Name
#
# = Synopsis
#
# = Description
#
# = Todo
# == Upcoming Features
# 0. Nothing yet.
# == Known Issues
# 0. Nothing yet.
# = References
# 0. <i>Devise Gem: https://github.com/plataformatec/devise</i>
#
#--
# Copyright (c) $today.year. SmithMicro Software, Inc.
# All Rights Reserved.
#++
# = Authors
# Author:: Luis Ramos (mailto:lramos@smithmicro.com)
# Author:: I-Ching Fong (mailto:ifong@smithmicro.com)
#
class User < ActiveRecord::Base
  # Include default devise modules. Others available are:
  # :token_authenticatable, :confirmable, :lockable and :timeoutable
  devise :database_authenticatable, :registerable,
         :recoverable, :rememberable, :trackable, :validatable,
         :token_authenticatable

  has_many :groups
  has_and_belongs_to_many :roles, :join_table => "roles_users"

  # Setup accessible (or protected) attributes for your model
  attr_accessible :first_name, :last_name, :email, :password, :password_confirmation, :remember_me
  attr_accessible :roles

  # The +add_role+ class method adds a role to the user's roles.
  # === Parameters
  # * _role_ = role : Role
  # === Example
  #  User.add_role(admin_role)
  def add_role(role)
    roles << role
  end

  # The +remove_role+ class method removes a role to the user's roles.
  # === Parameters
  # * _role_ = role : Role
  # === Example
  #  User.remove_role(admin_role)
  def remove_role(role)
    roles.delete(role)
  end

  def role?(role)
    return !!self.roles.find_by_name(role.to_s.camelize)
  end

  def has_role?(role)
    roles.exists?(role.id)
  end

  def roles_names
    roles.collect{ |r| r.name }.join ', '
  end

  def has_permission?(permission)
    permissions = roles.collect{ |r| r.permissions }.flatten
    !permissions.index(permission).nil?
  end

  def self.search(search)
    if search
      where('email LIKE ?', "%#{search}%")
    else
      scoped
    end
  end
end
