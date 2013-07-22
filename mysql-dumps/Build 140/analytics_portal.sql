/*
MySQL Data Transfer
Source Host: 127.0.0.1
Source Database: analytics_portal
Target Host: 127.0.0.1
Target Database: analytics_portal
Date: 9/7/2011 2:06:05 PM
*/

SET FOREIGN_KEY_CHECKS=0;
-- ----------------------------
-- Table structure for configurations
-- ----------------------------
DROP TABLE IF EXISTS `configurations`;
CREATE TABLE `configurations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `locale` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `time_zone` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `default_dashboard_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for dashboard_reports
-- ----------------------------
DROP TABLE IF EXISTS `dashboard_reports`;
CREATE TABLE `dashboard_reports` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dashboard_id` int(11) DEFAULT NULL,
  `report_id` int(11) DEFAULT NULL,
  `added_at` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `size` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `position` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=141 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for dashboards
-- ----------------------------
DROP TABLE IF EXISTS `dashboards`;
CREATE TABLE `dashboards` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `owner_id` int(11) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `filter_prefs` text COLLATE utf8_unicode_ci,
  `shared` tinyint(1) DEFAULT '0',
  `position` int(11) DEFAULT NULL,
  `filter_set_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for data_segment_job_runners
-- ----------------------------
DROP TABLE IF EXISTS `data_segment_job_runners`;
CREATE TABLE `data_segment_job_runners` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_segment_id` int(11) DEFAULT NULL,
  `job_runner_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for data_segments
-- ----------------------------
DROP TABLE IF EXISTS `data_segments`;
CREATE TABLE `data_segments` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `data_source_id` int(11) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `properties` text COLLATE utf8_unicode_ci,
  `schedule` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `enable_schedule` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=34 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for data_sources
-- ----------------------------
DROP TABLE IF EXISTS `data_sources`;
CREATE TABLE `data_sources` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `url` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `database_name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for delayed_jobs
-- ----------------------------
DROP TABLE IF EXISTS `delayed_jobs`;
CREATE TABLE `delayed_jobs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `priority` int(11) DEFAULT '0',
  `attempts` int(11) DEFAULT '0',
  `handler` text COLLATE utf8_unicode_ci,
  `last_error` text COLLATE utf8_unicode_ci,
  `run_at` datetime DEFAULT NULL,
  `locked_at` datetime DEFAULT NULL,
  `failed_at` datetime DEFAULT NULL,
  `locked_by` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `delayed_jobs_priority` (`priority`,`run_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for filter_params
-- ----------------------------
DROP TABLE IF EXISTS `filter_params`;
CREATE TABLE `filter_params` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `optionable_id` int(11) DEFAULT NULL,
  `filter_param_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `column` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `default_period` int(11) DEFAULT NULL,
  `min_period` int(11) DEFAULT '604800',
  `values` text COLLATE utf8_unicode_ci,
  `date_column` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `default_name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `report_query_id` int(11) DEFAULT NULL,
  `interval` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `optionable_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `enable_dynamic_values` tinyint(1) DEFAULT '0',
  `dynamic_values_query_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for filter_prefs
-- ----------------------------
DROP TABLE IF EXISTS `filter_prefs`;
CREATE TABLE `filter_prefs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `filter_param_id` int(11) DEFAULT NULL,
  `optionable_id` int(11) DEFAULT NULL,
  `optionable_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `values` text COLLATE utf8_unicode_ci,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `period` int(11) DEFAULT NULL,
  `selected` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `filter_enabled` tinyint(1) DEFAULT NULL,
  `enable_email` tinyint(1) DEFAULT NULL,
  `alert_direction` int(11) DEFAULT NULL,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `target` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=64 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for filter_sets
-- ----------------------------
DROP TABLE IF EXISTS `filter_sets`;
CREATE TABLE `filter_sets` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` text COLLATE utf8_unicode_ci,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for permissions
-- ----------------------------
DROP TABLE IF EXISTS `permissions`;
CREATE TABLE `permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `ability` int(11) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for permissions_roles
-- ----------------------------
DROP TABLE IF EXISTS `permissions_roles`;
CREATE TABLE `permissions_roles` (
  `permission_id` int(11) DEFAULT NULL,
  `role_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for queries
-- ----------------------------
DROP TABLE IF EXISTS `queries`;
CREATE TABLE `queries` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_segment_id` int(11) DEFAULT NULL,
  `query_string` text COLLATE utf8_unicode_ci,
  `store_in_db` tinyint(1) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `query_result_id` int(11) DEFAULT NULL,
  `sample_data` text COLLATE utf8_unicode_ci,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=85 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for report_links
-- ----------------------------
DROP TABLE IF EXISTS `report_links`;
CREATE TABLE `report_links` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `report_id` int(11) DEFAULT NULL,
  `linked_report_id` int(11) DEFAULT NULL,
  `filter_columns` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for report_queries
-- ----------------------------
DROP TABLE IF EXISTS `report_queries`;
CREATE TABLE `report_queries` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `report_id` int(11) DEFAULT NULL,
  `query_id` int(11) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `chart_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `chart_params` text COLLATE utf8_unicode_ci,
  `x_column` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `y_column` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `radius_column` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `name_column` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `query_select_group` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `sql_string` text COLLATE utf8_unicode_ci,
  `y_axis` int(11) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=51 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for reports
-- ----------------------------
DROP TABLE IF EXISTS `reports`;
CREATE TABLE `reports` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `highcharts_params` text COLLATE utf8_unicode_ci,
  `type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `table_columns` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `sample_option` int(11) DEFAULT '0',
  `filter_set_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for roles
-- ----------------------------
DROP TABLE IF EXISTS `roles`;
CREATE TABLE `roles` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `description` text COLLATE utf8_unicode_ci,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for roles_users
-- ----------------------------
DROP TABLE IF EXISTS `roles_users`;
CREATE TABLE `roles_users` (
  `role_id` int(11) DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for schema_migrations
-- ----------------------------
DROP TABLE IF EXISTS `schema_migrations`;
CREATE TABLE `schema_migrations` (
  `version` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  UNIQUE KEY `unique_schema_migrations` (`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for taggings
-- ----------------------------
DROP TABLE IF EXISTS `taggings`;
CREATE TABLE `taggings` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tag_id` int(11) DEFAULT NULL,
  `taggable_id` int(11) DEFAULT NULL,
  `taggable_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `tagger_id` int(11) DEFAULT NULL,
  `tagger_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `context` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `index_taggings_on_tag_id` (`tag_id`),
  KEY `index_taggings_on_taggable_id_and_taggable_type_and_context` (`taggable_id`,`taggable_type`,`context`)
) ENGINE=InnoDB AUTO_INCREMENT=34 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for tags
-- ----------------------------
DROP TABLE IF EXISTS `tags`;
CREATE TABLE `tags` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for user_dashboards
-- ----------------------------
DROP TABLE IF EXISTS `user_dashboards`;
CREATE TABLE `user_dashboards` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) DEFAULT NULL,
  `dashboard_id` int(11) DEFAULT NULL,
  `created_by` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Table structure for users
-- ----------------------------
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `last_name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `email` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `encrypted_password` varchar(128) COLLATE utf8_unicode_ci DEFAULT '',
  `reset_password_token` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `reset_password_sent_at` datetime DEFAULT NULL,
  `remember_created_at` datetime DEFAULT NULL,
  `sign_in_count` int(11) DEFAULT '0',
  `current_sign_in_at` datetime DEFAULT NULL,
  `last_sign_in_at` datetime DEFAULT NULL,
  `current_sign_in_ip` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `last_sign_in_ip` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `invitation_token` varchar(60) COLLATE utf8_unicode_ci DEFAULT NULL,
  `invitation_sent_at` datetime DEFAULT NULL,
  `invitation_limit` int(11) DEFAULT NULL,
  `invited_by_id` int(11) DEFAULT NULL,
  `invited_by_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `global_admin` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_users_on_email` (`email`),
  UNIQUE KEY `index_users_on_reset_password_token` (`reset_password_token`),
  KEY `index_users_on_first_name` (`first_name`),
  KEY `index_users_on_last_name` (`last_name`),
  KEY `index_users_on_invitation_token` (`invitation_token`),
  KEY `index_users_on_invited_by_id` (`invited_by_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Records 
-- ----------------------------
INSERT INTO `configurations` VALUES ('1', 'Abel\'s Company Analytics', 'en', 'Pacific Time (US & Canada)', '2011-08-31 01:28:04', '2011-09-07 17:36:11', '1');
INSERT INTO `dashboard_reports` VALUES ('1', '1', '3', null, '2011-05-30 00:31:18', '2011-05-30 00:31:18', 'large', '1');
INSERT INTO `dashboard_reports` VALUES ('94', '1', '16', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('95', '1', '5', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('96', '1', '1', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '3');
INSERT INTO `dashboard_reports` VALUES ('97', '1', '14', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '4');
INSERT INTO `dashboard_reports` VALUES ('98', '1', '13', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '5');
INSERT INTO `dashboard_reports` VALUES ('99', '2', '3', null, '2011-05-30 00:31:18', '2011-05-30 00:31:18', 'large', '1');
INSERT INTO `dashboard_reports` VALUES ('100', '2', '16', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('101', '2', '5', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('102', '2', '1', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '3');
INSERT INTO `dashboard_reports` VALUES ('103', '2', '14', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '4');
INSERT INTO `dashboard_reports` VALUES ('104', '2', '13', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '5');
INSERT INTO `dashboard_reports` VALUES ('105', '3', '3', null, '2011-05-30 00:31:18', '2011-05-30 00:31:18', 'large', '1');
INSERT INTO `dashboard_reports` VALUES ('106', '3', '16', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('107', '3', '5', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('108', '3', '1', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '3');
INSERT INTO `dashboard_reports` VALUES ('109', '3', '14', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '4');
INSERT INTO `dashboard_reports` VALUES ('110', '3', '13', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '5');
INSERT INTO `dashboard_reports` VALUES ('111', '4', '3', null, '2011-05-30 00:31:18', '2011-05-30 00:31:18', 'large', '1');
INSERT INTO `dashboard_reports` VALUES ('112', '4', '16', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('113', '4', '5', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('114', '4', '1', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '3');
INSERT INTO `dashboard_reports` VALUES ('115', '4', '14', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '4');
INSERT INTO `dashboard_reports` VALUES ('116', '4', '13', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '5');
INSERT INTO `dashboard_reports` VALUES ('117', '5', '3', null, '2011-05-30 00:31:18', '2011-05-30 00:31:18', 'large', '1');
INSERT INTO `dashboard_reports` VALUES ('118', '5', '16', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('119', '5', '5', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('120', '5', '1', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '3');
INSERT INTO `dashboard_reports` VALUES ('121', '5', '14', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '4');
INSERT INTO `dashboard_reports` VALUES ('122', '5', '13', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '5');
INSERT INTO `dashboard_reports` VALUES ('123', '6', '3', null, '2011-05-30 00:31:18', '2011-05-30 00:31:18', 'large', '1');
INSERT INTO `dashboard_reports` VALUES ('124', '6', '16', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('125', '6', '5', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('126', '6', '1', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '3');
INSERT INTO `dashboard_reports` VALUES ('127', '6', '14', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '4');
INSERT INTO `dashboard_reports` VALUES ('128', '6', '13', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '5');
INSERT INTO `dashboard_reports` VALUES ('129', '7', '3', null, '2011-05-30 00:31:18', '2011-05-30 00:31:18', 'large', '1');
INSERT INTO `dashboard_reports` VALUES ('130', '7', '16', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('131', '7', '5', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('132', '7', '1', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '3');
INSERT INTO `dashboard_reports` VALUES ('133', '7', '14', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '4');
INSERT INTO `dashboard_reports` VALUES ('134', '7', '13', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '5');
INSERT INTO `dashboard_reports` VALUES ('135', '8', '3', null, '2011-05-30 00:31:18', '2011-05-30 00:31:18', 'large', '1');
INSERT INTO `dashboard_reports` VALUES ('136', '8', '16', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('137', '8', '5', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '2');
INSERT INTO `dashboard_reports` VALUES ('138', '8', '1', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '3');
INSERT INTO `dashboard_reports` VALUES ('139', '8', '14', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '4');
INSERT INTO `dashboard_reports` VALUES ('140', '8', '13', null, '2011-07-21 07:51:25', '2011-07-21 07:51:25', 'large', '5');
INSERT INTO `dashboards` VALUES ('1', 'My Dashboard', '2', '2011-05-29 07:46:31', '2011-09-07 17:15:47', null, '0', '3', '1');
INSERT INTO `dashboards` VALUES ('2', 'My Dashboard', '1', '2011-08-31 01:30:04', '2011-08-31 01:32:25', null, '0', '1', '1');
INSERT INTO `dashboards` VALUES ('3', 'My Dashboard', '6', '2011-08-31 01:43:08', '2011-08-31 01:43:08', null, '0', '1', '1');
INSERT INTO `dashboards` VALUES ('4', 'My Dashboard', '9', '2011-08-31 01:43:57', '2011-08-31 01:43:58', null, '0', '1', '1');
INSERT INTO `dashboards` VALUES ('5', 'My Dashboard', '8', '2011-08-31 23:57:33', '2011-08-31 23:57:33', null, '0', '1', '1');
INSERT INTO `dashboards` VALUES ('6', 'WiFi Dashboard', '9', '2011-09-02 12:13:34', '2011-09-02 12:13:34', null, '0', '1', '2');
INSERT INTO `dashboards` VALUES ('7', 'WiFi Dashboard', '2', '2011-09-07 17:10:19', '2011-09-07 17:10:19', null, '0', '1', '2');
INSERT INTO `dashboards` VALUES ('8', '3G Dashboard', '2', '2011-09-07 17:12:02', '2011-09-07 17:12:02', null, '0', '1', '3');
INSERT INTO `data_segment_job_runners` VALUES ('1', '30', '36');
INSERT INTO `data_segments` VALUES ('1', 'Weekly Usage Trend 1', '', '1', '2011-05-27 00:51:12', '2011-06-27 23:05:24', '', '', '0');
INSERT INTO `data_segments` VALUES ('2', 'Error Scatterplot', '', '1', '2011-05-27 00:51:12', '2011-06-21 23:09:25', '', '', '0');
INSERT INTO `data_segments` VALUES ('3', 'Error Report', '', '1', '2011-05-27 00:51:12', '2011-07-18 20:23:58', '', '', '0');
INSERT INTO `data_segments` VALUES ('4', 'Data by Technology', '', '1', '2011-05-27 00:51:12', '2011-06-27 21:05:49', '{ \"start_date_month\": \"2010-01\", \"end_date_month\": \"2010-02\"}', '', '0');
INSERT INTO `data_segments` VALUES ('5', 'Daily Usage 1', '', '1', '2011-05-27 00:51:12', '2011-06-27 23:07:42', '', '', '0');
INSERT INTO `data_segments` VALUES ('6', 'Licensing Report 1 - New Licenses', '', '1', '2011-05-27 00:51:12', '2011-06-22 01:04:51', '', '', '0');
INSERT INTO `data_segments` VALUES ('7', 'Usage Trend 1', '', '1', '2011-05-27 00:51:12', '2011-06-27 22:56:03', '', '', '0');
INSERT INTO `data_segments` VALUES ('8', 'Device Usage Pie Chart', '', '1', '2011-05-27 00:51:12', '2011-07-15 18:54:06', '', '', '0');
INSERT INTO `data_segments` VALUES ('9', 'Weekly Usage Trend 3', '', '1', '2011-05-27 21:08:36', '2011-06-27 23:05:56', '', '', '0');
INSERT INTO `data_segments` VALUES ('10', 'Daily Usage 3', '', '1', '2011-05-27 22:38:39', '2011-06-27 22:55:05', '', '', '0');
INSERT INTO `data_segments` VALUES ('11', 'Technology Performance Report', '', '1', '2011-05-28 06:41:21', '2011-07-18 20:46:18', '', '', '0');
INSERT INTO `data_segments` VALUES ('12', 'Technology Performance Summary', '', '1', '2011-05-28 21:01:51', '2011-07-18 20:54:22', '', '', '0');
INSERT INTO `data_segments` VALUES ('13', 'Device Usage Report', '', '1', '2011-05-29 01:01:14', '2011-06-29 20:23:55', '', '', '0');
INSERT INTO `data_segments` VALUES ('14', 'Device Usage Summary', '', '1', '2011-05-29 01:05:06', '2011-07-06 23:19:24', '', '', '0');
INSERT INTO `data_segments` VALUES ('15', 'Usage Trend 3', '', '1', '2011-05-29 03:36:01', '2011-06-27 22:57:35', '', '', '0');
INSERT INTO `data_segments` VALUES ('16', 'Error Summary', '', '1', '2011-05-29 06:28:00', '2011-05-29 06:28:00', '', null, '0');
INSERT INTO `data_segments` VALUES ('17', 'Total Connections Heat Map', '', '1', '2011-06-07 21:22:33', '2011-06-10 11:51:50', '', '* * * * *', '0');
INSERT INTO `data_segments` VALUES ('18', 'KPI - License Data', '', '1', '2011-06-13 23:06:16', '2011-06-13 23:06:16', '', '', '0');
INSERT INTO `data_segments` VALUES ('19', 'Usage Trend 2', '', '1', '2011-06-20 21:11:05', '2011-06-20 21:11:05', '', '', '0');
INSERT INTO `data_segments` VALUES ('20', 'Weekly Usage Trend 2', '', '1', '2011-06-20 21:14:33', '2011-06-20 21:14:33', '', '', '0');
INSERT INTO `data_segments` VALUES ('21', 'Daily Usage 2', '', '1', '2011-06-20 23:23:12', '2011-06-20 23:23:12', '', '', '0');
INSERT INTO `data_segments` VALUES ('22', 'Licensing Report 2 - Returning Licenses', '', '1', '2011-06-22 08:09:27', '2011-06-22 08:09:27', '', '', '0');
INSERT INTO `data_segments` VALUES ('23', 'Licensing Report 3 - Lost Licenses', '', '1', '2011-06-22 08:17:13', '2011-06-22 08:17:13', '', '', '0');
INSERT INTO `data_segments` VALUES ('24', 'KPI - Technology Data - Connects Per Month', '', '1', '2011-07-06 20:32:49', '2011-07-06 20:36:27', '', '', '0');
INSERT INTO `data_segments` VALUES ('25', 'KPI - Technology Data - Connects Per Day', '', '1', '2011-07-06 20:37:53', '2011-07-06 20:37:53', '', '', '0');
INSERT INTO `data_segments` VALUES ('26', 'Connections per Day Heat Map ', 'Connections per Day Heat Map ', '1', '2011-07-06 21:16:11', '2011-07-06 21:16:11', '', '', '0');
INSERT INTO `data_segments` VALUES ('27', 'Total Throughput Heat Map ', 'Total Throughput Heat Map ', '1', '2011-07-06 22:28:34', '2011-07-06 22:28:34', '', '', '0');
INSERT INTO `data_segments` VALUES ('28', 'Throughput per Day Heat Map', 'Throughput per Day Heat Map', '1', '2011-07-06 22:44:42', '2011-07-06 22:44:42', '', '', '0');
INSERT INTO `data_segments` VALUES ('29', 'Average Signal Strength Heat Map', 'Average Signal Strength Heat Map', '1', '2011-07-06 22:50:34', '2011-07-06 22:50:34', '', '', '0');
INSERT INTO `data_segments` VALUES ('30', 'Average Peak Download Speed Heat Map', 'Average Peak Download Speed Heat Map', '1', '2011-07-06 22:55:20', '2011-07-06 22:55:20', '', '', '0');
INSERT INTO `data_segments` VALUES ('31', 'Error Frequency Heat Map', 'Error Frequency Heat Map', '1', '2011-07-06 23:00:03', '2011-07-06 23:00:03', '', '', '0');
INSERT INTO `data_segments` VALUES ('32', 'Connection Failure Rate Heat Map', 'Connection Failure Rate Heat Map', '1', '2011-07-06 23:04:15', '2011-07-06 23:04:15', '', '', '0');
INSERT INTO `data_segments` VALUES ('33', '* Login Data', '', '1', '2011-07-21 07:10:16', '2011-07-21 07:14:22', '', '', '0');
INSERT INTO `data_sources` VALUES ('1', 'analytics-admin', 'http://analytics-admin:3001', '2011-05-27 00:51:11', '2011-06-28 16:07:24', 'bouygues');
INSERT INTO `filter_params` VALUES ('1', '4', 'month_date', 'server_date', '2011-05-31 14:28:48', '2011-06-27 21:05:14', '31557600', '31557600', '', '', '', null, 'day', null, '0', null);
INSERT INTO `filter_params` VALUES ('2', '3', 'day_date', 'c_date', '2011-06-06 08:29:40', '2011-06-27 21:14:14', '31557600', '31557600', '', '', '', null, 'day', null, '0', null);
INSERT INTO `filter_params` VALUES ('3', '3', 'connection_type', 'device_type', '2011-06-06 08:48:17', '2011-07-21 00:39:10', '604800', '604800', '', '', '', null, 'day', null, '0', null);
INSERT INTO `filter_params` VALUES ('4', '3', 'check_boxes', 'manufacturer', '2011-06-06 12:46:49', '2011-06-27 21:14:14', '604800', '604800', '[\"Sierra Wireless, Inc.\", \"Dell\"]', '', '', null, 'day', null, '0', null);
INSERT INTO `filter_params` VALUES ('5', '3', 'check_boxes', 'device_model', '2011-06-07 06:08:33', '2011-07-21 00:39:10', '604800', '604800', '[\"foo1\", \"foo2\"]', '', '', null, 'day', null, '0', null);
INSERT INTO `filter_params` VALUES ('6', '11', 'day_date', 'ds', '2011-06-07 19:42:50', '2011-07-07 07:17:05', '31557600', '604800', '', '', '', null, 'day', null, '0', null);
INSERT INTO `filter_params` VALUES ('7', '1', 'day_date', 'c_date', '2011-06-08 22:34:17', '2011-06-27 23:48:31', '604800', '604800', '', '', '', null, 'day', null, '0', null);
INSERT INTO `filter_params` VALUES ('8', '16', 'kpi', 'cnt_current_total', '2011-06-14 10:33:34', '2011-07-06 21:40:26', '604800', '604800', '', 'server_date', 'Active Licenses', '35', 'month', null, '0', null);
INSERT INTO `filter_params` VALUES ('9', '16', 'kpi', 'throughput_per_day', '2011-07-06 21:40:26', '2011-07-06 21:40:45', '604800', '604800', '', 'server_date', 'Technology Data Throughput Per Day', '40', 'month', null, '0', null);
INSERT INTO `filter_params` VALUES ('10', '16', 'kpi', 'avg_signal_strength_percent', '2011-07-06 21:44:58', '2011-07-06 21:44:58', '604800', '604800', '', 'server_date', 'Technology Average Signal Strength', '40', 'month', null, '0', null);
INSERT INTO `filter_params` VALUES ('11', null, 'check_boxes', 'device_model', '2011-07-21 07:11:59', '2011-07-21 07:11:59', '604800', '604800', '', null, null, null, null, null, '1', '79');
INSERT INTO `filter_params` VALUES ('12', null, 'check_boxes', 'device_type', '2011-07-21 07:12:47', '2011-07-21 07:12:47', '604800', '604800', '[\"3GDevice\", \"4GDevice\", \"WiFiDevice\", \"EthernetDevice\"]', null, null, null, null, null, '0', null);
INSERT INTO `filter_params` VALUES ('13', '24', 'kpi', 'cnt_current_total', '2011-08-31 06:10:23', '2011-08-31 06:10:23', '7776000', '7776000', '', 'server_date', 'Active Licenses', '49', 'month', 'Report', '0', null);
INSERT INTO `filter_prefs` VALUES ('2', '2', '1', 'DashboardReport', null, '2011-06-06 08:30:09', '2011-07-06 00:07:33', '2011-05-01 00:00:00', '2011-06-30 00:00:00', '31557600', null, '0', '0', null, null, null, '2');
INSERT INTO `filter_prefs` VALUES ('3', '3', '1', 'DashboardReport', null, '2011-06-06 12:03:27', '2011-09-07 17:06:37', null, null, null, 'WiFiDevice', '0', '0', null, null, null, '2');
INSERT INTO `filter_prefs` VALUES ('4', '4', '1', 'DashboardReport', '--- !map:ActiveSupport::HashWithIndifferentAccess \nDell: \"1\"\nSierra Wireless, Inc.: \"1\"\n', '2011-06-06 13:11:13', '2011-07-05 19:13:22', null, null, null, null, '0', '0', null, null, null, '2');
INSERT INTO `filter_prefs` VALUES ('6', '5', '1', 'DashboardReport', '--- !map:ActiveSupport::HashWithIndifferentAccess \nfoo1: \"0\"\nfoo2: \"0\"\n', '2011-06-07 06:11:39', '2011-07-05 19:13:22', null, null, null, null, '0', '0', null, null, null, '2');
INSERT INTO `filter_prefs` VALUES ('8', '6', '11', 'Report', null, '2011-06-08 22:55:05', '2011-07-05 22:05:18', null, null, '31557600', null, '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('14', '8', '16', 'Report', null, '2011-06-17 00:41:50', '2011-09-01 00:45:08', null, null, null, null, '0', '1', '1', 'New License Count', '1000', null);
INSERT INTO `filter_prefs` VALUES ('18', '1', '4', 'Report', null, '2011-06-27 21:05:14', '2011-06-27 21:05:14', null, null, null, null, '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('19', '2', '3', 'Report', null, '2011-06-27 21:14:14', '2011-06-27 21:14:14', null, null, null, null, '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('20', '3', '3', 'Report', null, '2011-06-27 21:14:14', '2011-07-21 00:41:39', null, null, null, '', '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('21', '4', '3', 'Report', '--- !map:ActiveSupport::HashWithIndifferentAccess \nDell: \"0\"\nSierra Wireless, Inc.: \"0\"\n', '2011-06-27 21:14:14', '2011-06-27 21:14:14', null, null, null, null, '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('22', '5', '3', 'Report', '--- !map:ActiveSupport::HashWithIndifferentAccess \nfoo1: \"0\"\nfoo2: \"0\"\n', '2011-06-27 21:14:14', '2011-06-27 21:14:14', null, null, null, null, '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('23', '7', '1', 'Report', null, '2011-06-27 23:48:31', '2011-06-27 23:48:31', null, null, null, null, '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('41', '9', '16', 'Report', null, '2011-07-06 21:40:45', '2011-07-06 21:58:55', null, null, null, null, '0', '0', '0', 'Throughput Per Day', '10000000', null);
INSERT INTO `filter_prefs` VALUES ('42', '10', '16', 'Report', null, '2011-07-06 21:45:35', '2011-07-06 21:58:15', null, null, null, null, '0', '0', '1', 'Technology Average Signal Strength', '62', null);
INSERT INTO `filter_prefs` VALUES ('57', '11', '1', 'FilterSet', null, '2011-07-21 07:13:21', '2011-07-21 07:13:21', null, null, null, null, '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('58', '12', '1', 'FilterSet', '--- !map:ActiveSupport::HashWithIndifferentAccess \nWiFiDevice: \"0\"\n4GDevice: \"0\"\nEthernetDevice: \"0\"\n3GDevice: \"0\"\n', '2011-07-21 07:13:21', '2011-07-21 07:13:21', null, null, null, null, '0', '0', null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('59', '13', '24', 'Report', null, '2011-08-31 06:13:26', '2011-08-31 06:26:26', null, null, null, null, null, '0', '1', 'Data Target', '', null);
INSERT INTO `filter_prefs` VALUES ('60', '11', '2', 'FilterSet', '--- !map:ActiveSupport::HashWithIndifferentAccess \nU598: \"0\"\nWiMAX Adapter U1901: \"0\"\nUML290: \"0\"\nUltimate-N 6300 AGN: \"0\"\nBCM4318E: \"0\"\nUSB727 VERIZON: \"0\"\nHuawei E1552: \"0\"\nATHENA: \"0\"\nU600: \"0\"\nZTE MF190: \"0\"\n551L: \"0\"\nZTE MF633R: \"0\"\nWiMAX Adapter 250U: \"0\"\nWiFi Link 5100 AGN: \"0\"\nHuawei E180: \"0\"\nZTE MF668A: \"0\"\nU1901: \"0\"\nAlcatel X220L: \"0\"\n', '2011-09-02 12:13:02', '2011-09-02 12:13:02', null, null, null, null, '0', null, null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('61', '12', '2', 'FilterSet', '--- !map:ActiveSupport::HashWithIndifferentAccess \nWiFiDevice: \"1\"\n4GDevice: \"0\"\nEthernetDevice: \"0\"\n3GDevice: \"0\"\n', '2011-09-02 12:13:02', '2011-09-02 12:13:02', null, null, null, null, '1', null, null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('62', '11', '3', 'FilterSet', '--- !map:ActiveSupport::HashWithIndifferentAccess \nU598: \"0\"\nWiMAX Adapter U1901: \"0\"\nUML290: \"0\"\nUltimate-N 6300 AGN: \"0\"\nBCM4318E: \"0\"\nUSB727 VERIZON: \"0\"\nHuawei E1552: \"0\"\nATHENA: \"0\"\nU600: \"0\"\nZTE MF190: \"0\"\n551L: \"0\"\nZTE MF633R: \"0\"\nWiMAX Adapter 250U: \"0\"\nWiFi Link 5100 AGN: \"0\"\nHuawei E180: \"0\"\nZTE MF668A: \"0\"\nU1901: \"0\"\nAlcatel X220L: \"0\"\n', '2011-09-07 17:11:33', '2011-09-07 17:11:33', null, null, null, null, '0', null, null, null, null, null);
INSERT INTO `filter_prefs` VALUES ('63', '12', '3', 'FilterSet', '--- !map:ActiveSupport::HashWithIndifferentAccess \nWiFiDevice: \"0\"\n4GDevice: \"0\"\nEthernetDevice: \"0\"\n3GDevice: \"1\"\n', '2011-09-07 17:11:33', '2011-09-07 17:11:33', null, null, null, null, '1', null, null, null, null, null);
INSERT INTO `filter_sets` VALUES ('1', 'All Customers', '', '2011-07-21 07:13:21', '2011-07-21 07:13:21');
INSERT INTO `filter_sets` VALUES ('2', 'WiFi', '', '2011-09-02 12:13:02', '2011-09-02 12:13:02');
INSERT INTO `filter_sets` VALUES ('3', '3G', '', '2011-09-07 17:11:33', '2011-09-07 17:11:33');
INSERT INTO `permissions` VALUES ('1', 'View Analytics and Run Reports', '', '1', '2011-05-27 00:51:11', '2011-05-27 00:51:11');
INSERT INTO `permissions` VALUES ('2', 'Create Reports', '', '2', '2011-05-27 00:51:11', '2011-05-27 00:51:11');
INSERT INTO `permissions` VALUES ('3', 'Manage Users', '', '3', '2011-05-27 00:51:11', '2011-05-27 00:51:11');
INSERT INTO `permissions` VALUES ('4', 'Manager Roles', '', '4', '2011-05-27 00:51:11', '2011-05-27 00:51:11');
INSERT INTO `permissions` VALUES ('5', 'Configuration', '', '5', '2011-05-27 00:51:11', '2011-05-27 00:51:11');
INSERT INTO `permissions_roles` VALUES ('1', '1');
INSERT INTO `permissions_roles` VALUES ('1', '2');
INSERT INTO `permissions_roles` VALUES ('2', '1');
INSERT INTO `permissions_roles` VALUES ('3', '1');
INSERT INTO `permissions_roles` VALUES ('4', '1');
INSERT INTO `permissions_roles` VALUES ('5', '1');
INSERT INTO `queries` VALUES ('1', '7', 'SELECT a.sum_158_1 AS conn_per_day_wifi, a.sum_158_2 AS conn_per_day_cdma, a.sum_158_3 AS conn_per_day_wimax, a.sum_158_4 AS conn_per_day_ethernet, b.device_type AS device_type, a.c_date AS c_date, b.c_36 AS manufacturer, b.c_37 AS device_model FROM\r\n(\r\nSELECT SUM(c_158_1) AS sum_158_1, SUM(c_158_2) AS sum_158_2, SUM(c_158_3) AS sum_158_3, SUM(c_158_4) AS sum_158_4, to_date(server_date) AS c_date, uuid\r\nFROM LogEvents\r\nWHERE en = \"ApplicationInfo\"\r\nGROUP BY to_date(server_date), uuid\r\n) a\r\nJOIN\r\n(\r\nSELECT c_36, c_37, devicetype AS device_type, to_date(server_date) AS c_date, uuid\r\nFROM LogEvents\r\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\r\nGROUP BY c_36, c_37, devicetype, to_date(server_date), uuid\r\n) b\r\nON (a.c_date = b.c_date\r\n    AND a.uuid = b.uuid)', '0', '2011-05-27 00:51:12', '2011-07-20 00:40:41', '33', '', 'Usage Trend 1 - Connections / Day - Parent');
INSERT INTO `queries` VALUES ('2', '5', 'SELECT SUM(c_158_1) AS conn_per_day_wifi, SUM(c_158_2) AS conn_per_day_cdma, SUM(c_158_3) AS conn_per_day_wimax, SUM(c_158_4) AS conn_per_day_ethernet, devicetype AS device_type, to_date(server_date) AS c_date, Hour(server_date) AS hour FROM LogEvents\r\nwhere en = \"ApplicationInfo\"\r\nGROUP BY devicetype, to_date(server_date), Hour(server_date)', '0', '2011-05-27 00:51:12', '2011-07-19 18:06:41', '1', '', 'Daily Usage - Connections / Hour - Parent');
INSERT INTO `queries` VALUES ('3', '6', 'SELECT uuid, c_36 AS manufacturer, c_37 AS device_model, Year(server_date) AS year, Month(server_date) AS month\r\nFROM LogEvents\r\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\r\nGROUP BY uuid, c_36, c_37, Year(server_date), Month(server_date)', '0', '2011-05-27 00:51:12', '2011-07-06 22:16:11', '52', '', 'Licensing Report 1 - New Licenses - Parent');
INSERT INTO `queries` VALUES ('5', '5', 'SELECT SUM(conn_per_day_wifi) AS conn_per_hour_wifi, SUM(conn_per_day_cdma) AS conn_per_hour_cdma, SUM(conn_per_day_wimax) AS conn_per_hour_wimax, SUM(conn_per_day_ethernet) AS conn_per_hour_ethernet, hour, c_date\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY hour, c_date', '1', '2011-05-27 00:51:12', '2011-07-19 18:06:41', '2', '', 'Daily Usage - Connections / Hour - Child');
INSERT INTO `queries` VALUES ('6', '6', 'SELECT COUNT(uuid) AS cnt_current_total, concat(year,\"-\",month) AS server_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\nWHERE concat(year,\"-\",month) = \'{1_month_ago}\'\r\nGROUP BY concat(year,\"-\",month)\r\n', '0', '2011-05-27 00:51:12', '2011-07-06 22:16:11', '53', '', 'Licensing Report 1 - New Licenses - Child 1');
INSERT INTO `queries` VALUES ('7', '8', 'SELECT count(distinct uuid) as cnt_unique_uuid, c_36 AS manufacturer, c_37 AS device_model, to_date(server_date) AS c_date, 1 AS sum_helper\r\nFROM LogEvents\r\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\r\nGROUP BY c_36, c_37, to_date(server_date)', '0', '2011-05-27 00:51:12', '2011-07-15 18:54:06', '9', '', 'Device Usage - Parent');
INSERT INTO `queries` VALUES ('9', '6', 'SELECT COUNT(cur_uuid.uuid) AS cnt_returning, 1 AS sum_helper\r\nFROM\r\n(\r\nSELECT uuid\r\nFROM #HIVE_PARENT_TABLE\r\nWHERE concat(year,\"-\",month) = \'{1_month_ago}\'\r\nGROUP BY uuid\r\n) cur_uuid\r\nJOIN\r\n(\r\nSELECT uuid\r\nFROM #HIVE_PARENT_TABLE\r\nWHERE concat(year,\"-\",month) <= \'{2_months_ago}\'\r\nGROUP BY uuid\r\n) pre_uuid\r\nON (cur_uuid.uuid = pre_uuid.uuid)', '0', '2011-05-27 00:51:12', '2011-07-06 22:16:11', '54', '', 'Licensing Report 1 - New Licenses - Child 2');
INSERT INTO `queries` VALUES ('10', '1', 'SELECT SUM(c_158_1) AS conn_per_day_wifi, SUM(c_158_2) AS conn_per_day_cdma, SUM(c_158_3) AS conn_per_day_wimax, SUM(c_158_4) AS conn_per_day_ethernet, devicetype AS device_type, to_date(server_date) AS c_date, DayOfWeek(server_date) AS day_of_week\r\nFROM LogEvents\r\nWHERE en = \"ApplicationInfo\"\r\nGROUP BY devicetype, to_date(server_date), DayOfWeek(server_date)', '0', '2011-05-27 00:51:12', '2011-07-20 00:48:32', '39', '', 'Weekly Usage Trend 1 - Connections / Day (Weekly) - Parent');
INSERT INTO `queries` VALUES ('11', '3', 'SELECT distinct c_160_1 AS error_code_wifi, c_160_2 AS error_code_cdma, c_160_3 AS error_code_wimax, c_160_4 AS error_code_ethernet, c_161_1 AS error_msg_wifi, c_161_2 AS error_msg_cdma, c_161_3 AS error_msg_wimax, c_161_4 AS error_msg_ethernet, devicetype AS device_type, uuid, c_59 AS last_conn_timestamp_wifi, c_65 AS last_conn_timestamp_cdma, c_27 AS last_conn_timestamp_wimax, c_177 AS last_conn_timestamp_ethernet, c_58 AS first_conn_timestamp_wifi, c_64 AS first_conn_timestamp_cdma, c_26 AS first_conn_timestamp_wimax, c_176 AS first_conn_timestamp_ethernet, c_170_1 AS time_conn_wifi, c_170_2 AS time_conn_cdma, c_170_3 AS time_conn_wimax, c_170_4 AS time_conn_ethernet, to_date(server_date) AS c_date, 1 AS sum_helper\r\nFROM LogEvents', '0', '2011-05-27 00:51:12', '2011-07-18 20:23:58', '15', '', 'Error Report - Parent');
INSERT INTO `queries` VALUES ('12', '6', 'SELECT (a.cnt_current_total - b.cnt_returning) AS cnt_new, a.server_date AS server_date\r\nFROM #CHILD_1 a\r\nJOIN\r\n#CHILD_2 b\r\nON (a.sum_helper = b.sum_helper)', '1', '2011-05-27 00:51:12', '2011-07-06 22:16:11', '55', '{\r\n    \"columns\": [\r\n        \"server_date\",\r\n        \"cnt_new\"\r\n    ],\r\n    \"data\": [\r\n        [\r\n            \"2011-01\",\r\n            15\r\n        ],\r\n        [\r\n            \"2011-02\",\r\n            23\r\n        ],\r\n        [\r\n            \"2011-03\",\r\n            44\r\n        ],\r\n        [\r\n            \"2011-04\",\r\n            7\r\n        ],\r\n        [\r\n            \"2011-05\",\r\n            12\r\n        ],\r\n        [\r\n            \"2011-06\",\r\n            42\r\n        ],\r\n        [\r\n            \"2011-07\",\r\n            34\r\n        ],\r\n        [\r\n            \"2011-08\",\r\n            16\r\n        ],\r\n        [\r\n            \"2011-09\",\r\n            34\r\n        ],\r\n        [\r\n            \"2011-10\",\r\n            45\r\n        ],\r\n        [\r\n            \"2011-11\",\r\n            66\r\n        ],\r\n        [\r\n            \"2011-12\",\r\n            49\r\n        ]\r\n    ]\r\n}', 'Licensing Report 1 - New Licenses - Child 3');
INSERT INTO `queries` VALUES ('13', '1', 'SELECT SUM(conn_per_day_wifi) AS conn_per_day_wifi, SUM(conn_per_day_cdma) AS conn_per_day_cdma, SUM(conn_per_day_wimax) AS conn_per_day_wimax, SUM(conn_per_day_ethernet) conn_per_day_ethernet, day_of_week\r\nfrom #HIVE_PARENT_TABLE\r\nGROUP BY day_of_week', '1', '2011-05-27 00:51:12', '2011-07-20 00:48:32', '40', '', 'Weekly Usage Trend 1 - Connections / Day (Weekly) - Child');
INSERT INTO `queries` VALUES ('14', '2', 'SELECT distinct c_160_1 AS error_code_wifi, c_160_2 AS error_code_cdma, c_160_3 AS error_code_wimax, c_160_4 AS error_code_ethernet, c_161_1 AS error_msg_wifi, c_161_2 AS error_msg_cdma, c_161_3 AS error_msg_wimax, c_161_4 AS error_msg_ethernet, devicetype AS device_type, uuid, c_59 AS last_conn_timestamp_wifi, c_65 AS last_conn_timestamp_cdma, c_27 AS last_conn_timestamp_wimax, c_177 AS last_conn_timestamp_ethernet, c_58 AS first_conn_timestamp_wifi, c_64 AS first_conn_timestamp_cdma, c_26 AS first_conn_timestamp_wimax, c_176 AS first_conn_timestamp_ethernet, c_170_1 AS time_conn_wifi, c_170_2 AS time_conn_cdma, c_170_3 AS time_conn_wimax, c_170_4 AS time_conn_ethernet, to_date(server_date) AS c_date, 1 AS sum_helper\r\nFROM LogEvents', '0', '2011-05-27 00:51:12', '2011-07-20 01:28:20', '18', '{\"columns\":[\"x\",\"sum_avg_141_142\",\"radius_data\"],\"count\":3,\"data\":[[50,50,4],[15,40,7],[20,30,5]]}', 'Error Scatterplot - Parent');
INSERT INTO `queries` VALUES ('17', '4', 'SELECT AVG(c_141) AS avg_upload, AVG(c_142) AS avg_download, devicetype AS device_type, year(server_date) AS year, Month(server_date) AS month\r\nFROM LogEvents\r\nWHERE en = \"Disconnect\"\r\nGROUP BY devicetype, year(server_date), Month(server_date)', '0', '2011-05-27 00:51:12', '2011-07-15 18:51:04', '7', '', 'Data By Technology - Parent');
INSERT INTO `queries` VALUES ('18', '4', 'SELECT device_type, (avg_upload+avg_download) AS sum_avg_upload_download, concat(year,\"-\",month) AS server_date FROM #HIVE_PARENT_TABLE', '1', '2011-05-27 00:51:12', '2011-07-15 18:51:04', '8', '{\"columns\":[\"date\",\"sum_avg_141_142\",\"159\"],\"count\":3,\"data\":[[\"2011-01\",106,1],[\"2011-02\",107,1],[\"2011-03\",231,1],[\"2011-04\",133,1],[\"2011-05\",221,1],[\"2011-06\",667,1],[\"2011-07\",890,1],[\"2011-08\",924,1],[\"2011-09\",1025,1],[\"2011-10\",1215,1],[\"2011-11\",1025,1],[\"2011-12\",357,1]]}', 'Data By Technology - Child');
INSERT INTO `queries` VALUES ('21', '9', 'SELECT AVG(c_141) AS avg_upload, AVG(c_142) AS avg_download, devicetype AS device_type, to_date(server_date) AS c_date, DayOfWeek(server_date) AS day_of_week\r\nFROM LogEvents\r\nWHERE en = \"Disconnect\"\r\nGROUP BY devicetype, to_date(server_date), DayOfWeek(server_date)', '0', '2011-05-27 21:08:36', '2011-07-18 21:16:30', '43', '', 'Weekly Usage Trend 3 - Weekly Usage Trend Average Data Uploaded Downloaded - Parent');
INSERT INTO `queries` VALUES ('22', '9', 'SELECT (AVG(avg_upload) +  AVG(avg_download)) AS sum_avg_upload_download, day_of_week\r\nfrom #HIVE_PARENT_TABLE\r\nGROUP BY day_of_week', '1', '2011-05-27 21:08:36', '2011-07-18 21:16:30', '44', '', 'Weekly Usage Trend 3 - Average Data Uploaded Downloaded - Child');
INSERT INTO `queries` VALUES ('23', '10', 'SELECT AVG(c_141) AS avg_upload, AVG(c_142) AS avg_download, devicetype AS device_type, to_date(server_date) AS c_date, Hour(server_date) AS hour\r\nFROM LogEvents\r\nWHERE en = \"Disconnect\"\r\nGROUP BY devicetype, to_date(server_date), Hour(server_date)', '0', '2011-05-27 22:38:39', '2011-07-19 18:19:26', '5', '', 'Daily Usage 3 - Average Data Uploaded Downloaded - Parent');
INSERT INTO `queries` VALUES ('24', '10', 'SELECT (AVG(avg_upload) + AVG(avg_download)) AS sum_avg_upload_download, hour\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY hour', '1', '2011-05-27 22:38:39', '2011-07-15 18:47:37', '6', '', 'Daily Usage 3 - Average Data Uploaded Downloaded - Child');
INSERT INTO `queries` VALUES ('25', '8', 'SELECT (a.cnt_unique_uuid/b.total_unique_uuid) AS percent_uuid, a.c_date AS c_date, a.manufacturer AS manufacturer, a.device_model AS device_model\r\nFROM ( SELECT cnt_unique_uuid, c_date, sum_helper, manufacturer, device_model\r\nFROM #HIVE_PARENT_TABLE) a\r\nJOIN ( SELECT SUM(cnt_unique_uuid) AS total_unique_uuid, sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY sum_helper) b\r\nON (a.sum_helper = b.sum_helper)', '1', '2011-05-27 22:47:30', '2011-07-15 19:21:52', '10', '', 'Device Usage - Child');
INSERT INTO `queries` VALUES ('26', '11', 'SELECT devicetype AS device_type, SUM(c_158_1) AS conn_per_day_wifi, SUM(c_158_2) AS conn_per_day_cdma, SUM(c_158_3) AS conn_per_day_wimax, SUM(c_158_4) AS conn_per_day_ethernet, SUM(c_21) AS failed_conn_per_day_wifi, SUM(c_22) AS failed_conn_per_day_wwan, SUM(c_25) AS failed_conn_per_day_wimax, SUM(c_150) AS dropped_per_day, SUM(c_170_1) AS sum_170_1, SUM(c_170_2) AS sum_170_2, SUM(c_170_3) AS sum_170_3, SUM(c_170_4) AS sum_170_4, AVG(c_170_1) AS avg_170_1, AVG(c_170_2) AS avg_170_2, AVG(c_170_3) AS avg_170_3, AVG(c_170_4) AS avg_170_4, SUM(c_142) AS sum_142, SUM(c_141) AS sum_141, AVG(c_171_1) AS avg_171_1, AVG(c_171_2) AS avg_171_2, AVG(c_171_3) AS avg_171_3, AVG(c_171_4) AS avg_171_4, AVG(c_172_1) AS avg_172_1, AVG(c_172_2) AS avg_172_2, AVG(c_172_3) AS avg_172_3, AVG(c_172_4) AS avg_172_4, AVG(c_162_1) AS avg_162_1, AVG(c_162_2) AS avg_162_2, AVG(c_162_3) AS avg_162_3, AVG(c_162_4) AS avg_162_4, to_date(server_date) AS c_date\r\nfrom LogEvents\r\nGROUP BY devicetype, to_date(server_date)', '0', '2011-05-28 06:41:21', '2011-07-18 20:46:18', '28', '', 'Technology Performance Report - Parent');
INSERT INTO `queries` VALUES ('27', '11', 'SELECT device_type AS connection_type, conn_per_day_wifi, conn_per_day_cdma, conn_per_day_wimax, conn_per_day_ethernet, failed_conn_per_day_wifi, failed_conn_per_day_wwan, failed_conn_per_day_wimax, dropped_per_day, AVG(sum_170_1) AS avg_time_conn_per_day_wifi, AVG(sum_170_2) AS avg_time_conn_per_day_cdma, AVG(sum_170_3) AS avg_time_conn_per_day_wimax, AVG(sum_170_4) AS avg_time_conn_per_day_ethernet, avg_170_1 AS avg_time_conn_per_session_wifi, avg_170_2 AS avg_time_conn_per_session_cdma, avg_170_3 AS avg_time_conn_per_session_wimax, avg_170_4 AS avg_time_conn_per_session_ethernet, AVG(sum_142) AS avg_downloaded_per_day, AVG(sum_141) AS avg_uploaded_per_day, avg_171_1 AS avg_download_speed_wifi, avg_171_2 AS avg_download_speed_cdma, avg_171_3 AS avg_download_speed_wimax, avg_171_4 AS avg_download_speed_ethernet, avg_172_1 AS avg_upload_speed_wifi, avg_172_2 AS avg_upload_speed_cdma, avg_172_3 AS avg_upload_speed_wimax, avg_172_4 AS avg_upload_speed_ethernet, avg_162_1 AS avg_signal_strength_wifi, avg_162_2 AS avg_signal_strength_cdma, avg_162_3 AS avg_signal_strength_wimax, avg_162_4 AS avg_signal_strength_ethernet, c_date AS c_date\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY device_type, conn_per_day_wifi, conn_per_day_cdma, conn_per_day_wimax, conn_per_day_ethernet, failed_conn_per_day_wifi, failed_conn_per_day_wwan, failed_conn_per_day_wimax, dropped_per_day, avg_170_1, avg_170_2, avg_170_3, avg_170_4, avg_171_1, avg_171_2, avg_171_3, avg_171_4, avg_172_1, avg_172_2, avg_172_3, avg_172_4, avg_162_1, avg_162_2, avg_162_3, avg_162_4, c_date', '1', '2011-05-28 06:41:21', '2011-07-20 00:16:48', '29', '', 'Technology Performance Report - Child');
INSERT INTO `queries` VALUES ('28', '12', 'SELECT devicetype AS device_type, SUM(c_158_1) AS sum_158_1, SUM(c_158_2) AS sum_158_2, SUM(c_158_3) AS sum_158_3, SUM(c_158_4) AS sum_158_4, SUM(c_141) AS sum_141, SUM(c_142) AS sum_142, AVG(c_171_1) AS avg_171_1, AVG(c_171_2) AS avg_171_2, AVG(c_171_3) AS avg_171_3, AVG(c_171_4) AS avg_171_4, AVG(c_162_1) AS avg_162_1, AVG(c_162_2) AS avg_162_2, AVG(c_162_3) AS avg_162_3, AVG(c_162_4) AS avg_162_4, to_date(server_date) AS c_date\r\nFROM LogEvents\r\nGROUP BY devicetype, to_date(server_date)', '0', '2011-05-28 21:01:51', '2011-07-18 20:54:22', '30', '', 'Technology Performance Summary - Parent');
INSERT INTO `queries` VALUES ('29', '12', 'SELECT device_type, sum_158_1 AS conn_attempts_wifi, sum_158_2 AS conn_attempts_cdma, sum_158_3 AS conn_attempts_wimax, sum_158_4 AS conn_attempts_ethernet, AVG(sum_141 + sum_142) AS throughput_per_day, avg_171_1 AS download_speed_wifi, avg_171_2 AS download_speed_cdma, avg_171_3 AS download_speed_wimax, avg_171_4 AS download_speed_ethernet, avg_162_1 AS signal_strength_percent_wifi, avg_162_2 AS signal_strength_percent_cdma, avg_162_3 AS signal_strength_percent_wimax, avg_162_4 AS signal_strength_percent_ethernet, c_date\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY device_type, sum_158_1, sum_158_2, sum_158_3, sum_158_4, avg_171_1, avg_171_2, avg_171_3, avg_171_4, avg_162_1, avg_162_2, avg_162_3, avg_162_4, c_date', '1', '2011-05-28 21:01:51', '2011-07-20 00:14:13', '31', '', 'Technology Performance Summary - Child');
INSERT INTO `queries` VALUES ('30', '13', 'SELECT uuid, ds, c_36 AS manufacturer, c_37 AS device_model FROM LogEvents where en=\'3GDeviceInsertion\' or en=\'4GDeviceInsertion\' group by uuid, ds, c_36, c_37', '0', '2011-05-29 01:01:14', '2011-07-20 01:06:35', '11', '', 'Device Usage Report - Parent');
INSERT INTO `queries` VALUES ('31', '13', 'SELECT\r\n        uuid,\r\n        ds,\r\n        devicetype AS device_type,\r\n        SUM(c_142) AS data_download_per_day,\r\n        SUM(c_141) AS data_upload_per_day,\r\n        SUM(((Hour(c_170_1) * 3600) + (minute(c_170_1) * 60) + (second(c_170_1)))) AS time_conn_per_day_wifi,\r\n        SUM(((Hour(c_170_2) * 3600) + (minute(c_170_2) * 60) + (second(c_170_2)))) AS time_conn_per_day_cdma,\r\n        SUM(((Hour(c_170_3) * 3600) + (minute(c_170_3) * 60) + (second(c_170_3)))) AS time_conn_per_day_wimax,\r\n        SUM(((Hour(c_170_4) * 3600) + (minute(c_170_4) * 60) + (second(c_170_4)))) AS time_conn_per_day_ethernet,\r\n        c_160_1 AS error_code_wifi,\r\n        c_160_2 AS error_code_cdma,\r\n        c_160_3 AS error_code_wimax,\r\n        c_160_4 AS error_code_ethernet,\r\n        AVG(c_171_1) AS avg_peek_download_speed_wifi,\r\n        AVG(c_171_2) AS avg_peek_download_speed_cdma,\r\n        AVG(c_171_3) AS avg_peek_download_speed_wimax,\r\n        AVG(c_171_4) AS avg_peek_download_speed_ethernet,\r\n        AVG(c_172_1) AS avg_peek_upload_speed_wifi,\r\n        AVG(c_172_2) AS avg_peek_upload_speed_cdma,\r\n        AVG(c_172_3) AS avg_peek_upload_speed_wimax,\r\n        AVG(c_172_4) AS avg_peek_upload_speed_ethernet\r\nFROM LogEvents\r\nWHERE en=\'Disconnect\' OR en=\'Connection\' OR en=\'ConnectivityError\' GROUP BY uuid, ds, devicetype, c_160_1, c_160_2, c_160_3, c_160_4', '0', '2011-05-29 01:01:14', '2011-07-20 01:35:04', '12', '', 'Device Usage Report - Child 1');
INSERT INTO `queries` VALUES ('32', '14', 'select uuid, ds, c_36 AS manufacturer, c_37 AS device_model from logevents where en=\'3GDeviceInsertion\' or en=\'4GDeviceInsertion\' group by uuid, ds, c_36, c_37', '0', '2011-05-29 01:05:06', '2011-07-18 20:14:55', '13', '', 'Device Usage Summary - Parent');
INSERT INTO `queries` VALUES ('33', '14', 'SELECT\r\n        uuid,\r\n        ds,\r\n        SUM(c_142) AS data_download_per_day,\r\n        SUM(c_141) AS data_upload_per_day,\r\n        SUM(((hour(c_170_1) * 3600) + (minute(c_170_1) * 60) + (second(c_170_1)))) AS time_conn_per_day_wifi,\r\n        SUM(((hour(c_170_2) * 3600) + (minute(c_170_2) * 60) + (second(c_170_2)))) AS time_conn_per_day_cdma,\r\n        SUM(((hour(c_170_3) * 3600) + (minute(c_170_3) * 60) + (second(c_170_3)))) AS time_conn_per_day_wimax,\r\n        SUM(((hour(c_170_4) * 3600) + (minute(c_170_4) * 60) + (second(c_170_4)))) AS time_conn_per_day_ethernet\r\nFROM logevents\r\nWHERE en=\'Disconnect\' group by uuid, ds', '0', '2011-05-29 01:05:06', '2011-07-07 03:30:55', '14', '', 'Device Usage Summary - Child 1');
INSERT INTO `queries` VALUES ('34', '7', 'SELECT conn_per_day_wifi, conn_per_day_cdma, conn_per_day_wimax, conn_per_day_ethernet, device_type, manufacturer, device_model, c_date\r\nFROM #HIVE_PARENT_TABLE', '1', '2011-05-29 03:22:47', '2011-07-18 20:59:44', '34', '', 'Usage Trend 1 - Connections Per Day - Child');
INSERT INTO `queries` VALUES ('35', '15', 'SELECT a.avg_141 AS avg_upload, a.avg_142 AS avg_download, a.device_type AS device_type, a.c_date AS c_date, b.c_36 AS manufacturer, b.c_37 AS device_model\r\nFROM\r\n(\r\nSELECT AVG(c_141) AS avg_141, AVG(c_142) AS avg_142, devicetype AS device_type, to_date(server_date) AS c_date, uuid\r\nFROM LogEvents\r\nWHERE en = \"Disconnect\"\r\nGROUP BY devicetype, to_date(server_date), uuid\r\n) a\r\nJOIN\r\n(\r\nSELECT c_36, c_37, devicetype AS device_type, to_date(server_date) AS c_date, uuid\r\nFROM LogEvents\r\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\r\nGROUP BY c_36, c_37, devicetype, to_date(server_date), uuid\r\n) b\r\nON (a.device_type = b.device_type\r\n                AND a.c_date = b.c_date\r\n                AND a.uuid = b.uuid)', '0', '2011-05-29 03:36:01', '2011-07-20 00:46:47', '37', '', 'Usage Trend 3 - Average Data Uploaded and Downloaded - Parent');
INSERT INTO `queries` VALUES ('36', '15', 'SELECT (avg_upload + avg_download) AS sum_avg_upload_download, device_type, manufacturer, device_model, c_date\r\nFROM #HIVE_PARENT_TABLE', '1', '2011-05-29 03:36:01', '2011-07-18 21:12:14', '38', '', 'Usage Trend 3 - Average Data Uploaded and Downloaded - Child');
INSERT INTO `queries` VALUES ('37', '3', 'SELECT distinct error_code, error_msg, device_type, uuid, last_conn_timestamp, first_conn_timestamp, time_conn, c_date, sum_helper FROM (\r\nSELECT distinct error_code_wifi AS error_code, error_msg_wifi AS error_msg, device_type, uuid, last_conn_timestamp_wifi AS last_conn_timestamp, first_conn_timestamp_wifi AS first_conn_timestamp, time_conn_wifi AS time_conn, c_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\nSELECT distinct error_code_cdma AS error_code, error_msg_cdma AS error_msg, device_type, uuid, last_conn_timestamp_cdma AS last_conn_timestamp, first_conn_timestamp_cdma AS first_conn_timestamp, time_conn_cdma AS time_conn, c_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\nSELECT distinct error_code_wimax AS error_code, error_msg_wimax AS error_msg, device_type, uuid, last_conn_timestamp_wimax AS last_conn_timestamp, first_conn_timestamp_wimax AS first_conn_timestamp, time_conn_wimax AS time_conn, c_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\nSELECT distinct error_code_ethernet AS error_code, error_msg_ethernet AS error_msg, device_type, uuid, last_conn_timestamp_ethernet AS last_conn_timestamp, first_conn_timestamp_ethernet AS first_conn_timestamp, time_conn_ethernet AS time_conn, c_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\n) a', '0', '2011-05-29 05:19:01', '2011-07-20 01:24:07', '16', '', 'Error Report - Child 1');
INSERT INTO `queries` VALUES ('38', '3', 'SELECT a.error_code AS error_code, a.error_msg AS error_msg, a.device_type AS device_type, (a.total_active_user_affected * 100 / b.total_active_user) AS percent_user_affected, a.frequency_per_hour AS frequency_per_hour, a.total_cnt_error AS total_cnt_error FROM\r\n\r\n(SELECT error_code, error_msg, device_type, COUNT(DISTINCT uuid) AS total_active_user_affected, (COUNT(error_code)/CASE SUM(time_conn) WHEN 0.0 THEN 1.0 ELSE SUM(time_conn) END) AS frequency_per_hour, COUNT(error_code) AS total_cnt_error, sum_helper\r\nFROM #PREVIOUS\r\nWHERE error_code != \'\'\r\nGROUP BY error_code, error_msg, device_type, sum_helper) a\r\n\r\nJOIN\r\n\r\n(SELECT COUNT(DISTINCT uuid) AS total_active_user, sum_helper\r\nFROM #PREVIOUS\r\nGROUP BY sum_helper) b\r\n\r\nON (a.sum_helper = b.sum_helper)', '1', '2011-05-29 05:19:59', '2011-07-20 01:24:07', '17', '', 'Error Report - Child 2');
INSERT INTO `queries` VALUES ('39', '16', 'SELECT distinct c_160_1 AS error_code_wifi, c_160_2 AS error_code_cdma, c_160_3 AS error_code_wimax, c_160_4 AS error_code_ethernet, c_161_1 AS error_msg_wifi, c_161_2 AS error_msg_cdma, c_161_3 AS error_msg_wimax, c_161_4 AS error_msg_ethernet, uuid, c_59 AS last_conn_timestamp_wifi, c_65 AS last_conn_timestamp_cdma, c_27 AS last_conn_timestamp_wimax, c_177 AS last_conn_timestamp_ethernet, c_58 AS first_conn_timestamp_wifi, c_64 AS first_conn_timestamp_cdma, c_26 AS first_conn_timestamp_wimax, c_176 AS first_conn_timestamp_ethernet, c_170_1 AS time_conn_wifi, c_170_2 AS time_conn_cdma, c_170_3 AS time_conn_wimax, c_170_4 AS time_conn_ethernet, to_date(server_date) AS c_date, 1 AS sum_helper\nFROM LogEvents', '0', '2011-05-29 06:28:00', '2011-06-28 16:46:20', '21', '', 'Error Summary - Parent');
INSERT INTO `queries` VALUES ('40', '16', 'SELECT distinct error_code, error_msg, uuid, last_conn_timestamp, first_conn_timestamp, time_conn, c_date, sum_helper FROM (\nSELECT distinct error_code_wifi AS error_code, error_msg_wifi AS error_msg, uuid, last_conn_timestamp_wifi AS last_conn_timestamp, first_conn_timestamp_wifi AS first_conn_timestamp, time_conn_wifi AS time_conn, c_date, 1 AS sum_helper\nFROM #HIVE_PARENT_TABLE\nUNION ALL\nSELECT distinct error_code_cdma AS error_code, error_msg_cdma AS error_msg, uuid, last_conn_timestamp_cdma AS last_conn_timestamp, first_conn_timestamp_cdma AS first_conn_timestamp, time_conn_cdma AS time_conn, c_date, 1 AS sum_helper\nFROM #HIVE_PARENT_TABLE\nUNION ALL\nSELECT distinct error_code_wimax AS error_code, error_msg_wimax AS error_msg, uuid, last_conn_timestamp_wimax AS last_conn_timestamp, first_conn_timestamp_wimax AS first_conn_timestamp, time_conn_wimax AS time_conn, c_date, 1 AS sum_helper\nFROM #HIVE_PARENT_TABLE\nUNION ALL\nSELECT distinct error_code_ethernet AS error_code, error_msg_ethernet AS error_msg, uuid, last_conn_timestamp_ethernet AS last_conn_timestamp, first_conn_timestamp_ethernet AS first_conn_timestamp, time_conn_ethernet AS time_conn, c_date, 1 AS sum_helper\nFROM #HIVE_PARENT_TABLE\n) a', '0', '2011-05-29 06:28:58', '2011-06-28 16:46:20', '22', '', 'Error Summary - Child 1');
INSERT INTO `queries` VALUES ('41', '16', 'SELECT a.error_code AS error_code, a.error_msg AS error_msg, (a.total_active_user_affected * 100 / b.total_active_user) AS percent_user_affected, a.frequency_per_hour AS frequency_per_hour FROM\n(SELECT error_code, error_msg, COUNT(DISTINCT uuid) AS total_active_user_affected, (COUNT(error_code)/CASE SUM(time_conn) WHEN 0.0 THEN 1.0 ELSE SUM(time_conn) END) AS frequency_per_hour, sum_helper\nFROM #PREVIOUS\nWHERE error_code != \'\'\nGROUP BY error_code, error_msg, sum_helper) a\nJOIN\n(SELECT COUNT(DISTINCT uuid) AS total_active_user, sum_helper\nFROM #PREVIOUS\nGROUP BY sum_helper) b\nON (a.sum_helper = b.sum_helper)', '1', '2011-05-29 06:29:43', '2011-06-28 16:46:20', '23', '', 'Error Summary - Child 2');
INSERT INTO `queries` VALUES ('42', '17', 'SELECT devicetype  AS device_type,\r\nSUM(c_158_1) AS conn_attempts_wifi,\r\nSUM(c_158_2) AS conn_attempts_cdma,\r\nSUM(c_158_3) AS conn_attempts_wimax,\r\nSUM(c_158_4) AS conn_attempts_ethernet,\r\nYear(server_date) AS year,\r\nMonth(server_date) AS month,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM LogEvents\r\nGROUP BY devicetype, Year(server_date), Month(server_date) , heat_lat, heat_lng, heat_x, heat_y, heat_num', '0', '2011-06-07 21:22:33', '2011-07-18 20:55:31', '3', '', 'Total Connections Heat Map - Parent');
INSERT INTO `queries` VALUES ('43', '18', 'SELECT uuid, c_36 AS manufacturer, c_37 AS device_model, Year(server_date) AS year, Month(server_date) AS month\nFROM LogEvents\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\nGROUP BY uuid, c_36, c_37, Year(server_date), Month(server_date)\n', '0', '2011-06-13 23:06:16', '2011-06-28 16:46:24', '24', '', 'KPI - License Data - Parent');
INSERT INTO `queries` VALUES ('44', '18', 'SELECT COUNT(uuid) AS cnt_current_total, 1 AS sum_helper, concat(year,\"-\",month) AS server_date\nFROM #HIVE_PARENT_TABLE\nGROUP BY concat(year,\"-\",month)', '1', '2011-06-13 23:06:16', '2011-06-28 16:46:24', '25', '', 'KPI - License Data - Current Licenses');
INSERT INTO `queries` VALUES ('45', '18', 'SELECT COUNT(cur_uuid.uuid) AS cnt_returning, 1 AS sum_helper, cur_uuid.server_date\nFROM\n(\nSELECT uuid, concat(year,\"-\",month) AS server_date\nFROM #HIVE_PARENT_TABLE\n{where_1_month_ago}\nGROUP BY uuid, concat(year,\"-\",month)\n) cur_uuid\nJOIN\n(\nSELECT uuid \nFROM #HIVE_PARENT_TABLE\n{where_before_1_month_ago}\nGROUP BY uuid\n) pre_uuid\nON (cur_uuid.uuid = pre_uuid.uuid)\nGROUP BY cur_uuid.server_date', '1', '2011-06-13 23:39:04', '2011-06-28 16:46:24', '26', '', 'KPI - License Data - Returning Licenses');
INSERT INTO `queries` VALUES ('46', '18', 'SELECT SUM(a.cnt_current_total - b.cnt_returning) AS cnt_new, b.server_date\nFROM #CHILD_1 a\nJOIN\n#CHILD_2 b\nON (a.sum_helper = b.sum_helper)\nGROUP BY b.server_date', '1', '2011-06-14 01:59:23', '2011-06-28 16:46:24', '27', '', 'KPI - License Data - New Licenses');
INSERT INTO `queries` VALUES ('47', '19', 'SELECT b.c_21 AS conn_failure_cnt_wifi, b.c_57 AS conn_success_cnt_wifi, b.c_22 AS conn_failure_cnt_wwan, b.c_151 AS conn_success_cnt_wwan, b.c_25 AS conn_failure_cnt_wimax, b.c_24 AS conn_success_cnt_wimax, b.c_175 AS conn_failure_cnt_ethernet, b.c_174 AS conn_success_cnt_ethernet, a.c_150 AS dropped, a.device_type AS device_type, a.c_date AS c_date, b.c_36 AS manufacturer, b.c_37 AS device_model FROM\r\n(\r\nSELECT c_150, devicetype AS device_type, to_date(server_date) AS c_date, uuid\r\nFROM LogEvents\r\nWHERE en = \"Disconnect\"\r\nGROUP BY c_150, devicetype, to_date(server_date), uuid\r\n) a\r\nJOIN\r\n(\r\nSELECT c_21, c_57, c_24, c_25, c_22, c_151, c_174, c_175, c_36, c_37, devicetype AS device_type, to_date(server_date) AS c_date, uuid\r\nFROM LogEvents\r\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\r\nGROUP BY c_21, c_57, c_24, c_25, c_22, c_151, c_174, c_175, c_36, c_37, devicetype, to_date(server_date), uuid\r\n) b\r\nON (a.device_type = b.device_type\r\n                AND a.c_date = b.c_date\r\n                AND a.uuid = b.uuid)', '0', '2011-06-20 21:11:05', '2011-07-20 00:43:35', '35', '', 'Usage Trend - Failure Rate - Parent');
INSERT INTO `queries` VALUES ('48', '19', 'SELECT (conn_failure_cnt_wifi + dropped) / (conn_failure_cnt_wifi + dropped + conn_success_cnt_wifi) * 100 AS failure_rate_wifi, (conn_failure_cnt_wwan + dropped) / (conn_failure_cnt_wwan + dropped + conn_success_cnt_wwan) * 100 AS failure_rate_wwan, (conn_failure_cnt_wimax + dropped) / (conn_failure_cnt_wimax + dropped + conn_success_cnt_wimax) * 100 AS failure_rate_wimax, (conn_failure_cnt_ethernet + dropped) / (conn_failure_cnt_ethernet + dropped + conn_success_cnt_ethernet) * 100 AS failure_rate_ethernet, c_date\r\nfrom #HIVE_PARENT_TABLE\r\n', '1', '2011-06-20 21:11:05', '2011-07-18 21:07:22', '36', '', 'Usage Trend - Failure Rate - Child');
INSERT INTO `queries` VALUES ('49', '20', 'SELECT b.c_21 AS conn_failure_cnt_wifi, b.c_57 AS conn_success_cnt_wifi, b.c_22 AS conn_failure_cnt_wwan, b.c_151 AS conn_success_cnt_wwan, b.c_25 AS conn_failure_cnt_wimax, b.c_24 AS conn_success_cnt_wimax, b.c_175 AS conn_failure_cnt_ethernet, b.c_174 AS conn_success_cnt_ethernet, a.c_150 AS dropped, a.device_type AS device_type, a.day_of_week AS day_of_week, b.c_36 AS manufacturer, b.c_37 AS device_model FROM\r\n(\r\nSELECT c_150, devicetype AS device_type, DayOfWeek(server_date) AS day_of_week, uuid\r\nFROM LogEvents\r\nWHERE en = \"Disconnect\"\r\nGROUP BY c_150, devicetype, DayOfWeek(server_date), uuid\r\n) a\r\nJOIN\r\n(\r\nSELECT c_21, c_57, c_24, c_25, c_22, c_151, c_174, c_175, c_36, c_37, devicetype AS device_type, DayOfWeek(server_date) AS day_of_week, uuid\r\nFROM LogEvents\r\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDevieInsertion\" OR en=\"4GDevieInsertion\")\r\nGROUP BY c_21, c_57, c_24, c_25, c_22, c_151, c_174, c_175, c_36, c_37, devicetype, DayOfWeek(server_date), uuid\r\n) b\r\nON (a.device_type = b.device_type\r\n                AND a.day_of_week = b.day_of_week\r\n                AND a.uuid = b.uuid)', '0', '2011-06-20 21:14:33', '2011-07-20 00:51:43', '41', '', 'Weekly Usage Trend 2 - Failure Rate - Parent');
INSERT INTO `queries` VALUES ('50', '20', 'SELECT (conn_failure_cnt_wifi + dropped) / (conn_failure_cnt_wifi + dropped + conn_success_cnt_wifi) * 100 AS failure_rate_wifi, (conn_failure_cnt_wwan + dropped) / (conn_failure_cnt_wwan + dropped + conn_success_cnt_wwan) * 100 AS failure_rate_wwan, (conn_failure_cnt_wimax + dropped) / (conn_failure_cnt_wimax + dropped + conn_success_cnt_wimax) * 100 AS failure_rate_wimax, (conn_failure_cnt_ethernet + dropped) / (conn_failure_cnt_ethernet + dropped + conn_success_cnt_ethernet) * 100 AS failure_rate_ethernet, day_of_week\r\nfrom #HIVE_PARENT_TABLE\r\n', '1', '2011-06-20 21:14:33', '2011-07-18 21:15:20', '42', '', 'Weekly Usage Trend 2 - Failure Rate - Child');
INSERT INTO `queries` VALUES ('51', '21', 'SELECT b.c_21 AS conn_failure_cnt_wifi, b.c_57 AS conn_success_cnt_wifi, b.c_22 AS conn_failure_cnt_wwan, b.c_151 AS conn_success_cnt_wwan, b.c_25 AS conn_failure_cnt_wimax, b.c_24 AS conn_success_cnt_wimax, b.c_175 AS conn_failure_cnt_ethernet, b.c_174 AS conn_success_cnt_ethernet, a.c_150 AS dropped, a.device_type AS device_type, a.c_date AS c_date, a.hour AS hour, b.c_36 AS manufacturer, b.c_37 AS device_model FROM\r\n(\r\nSELECT c_150, devicetype AS device_type, to_date(server_date) AS c_date, Hour(server_date) AS hour, uuid\r\nFROM LogEvents\r\nWHERE en = \"Disconnect\"\r\nGROUP BY c_150, devicetype, to_date(server_date), Hour(server_date), uuid\r\n) a\r\nJOIN\r\n(\r\nSELECT c_21, c_57, c_24, c_25, c_22, c_151, c_174, c_175, c_36, c_37, devicetype AS device_type, to_date(server_date) AS c_date, Hour(server_date) AS hour, uuid\r\nFROM LogEvents\r\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\r\nGROUP BY c_21, c_57, c_24, c_25, c_22, c_151, c_174, c_175, c_36, c_37, devicetype, to_date(server_date), Hour(server_date), uuid\r\n) b\r\nON (a.device_type = b.device_type\r\n                AND a.c_date = b.c_date\r\n                AND a.uuid = b.uuid)', '0', '2011-06-20 23:23:12', '2011-07-19 18:11:16', '3', '', 'Daily Usage - Failure Rate - Parent');
INSERT INTO `queries` VALUES ('52', '21', 'SELECT (conn_failure_cnt_wifi + dropped) / (conn_failure_cnt_wifi + dropped + conn_success_cnt_wifi) * 100 AS failure_rate_wifi, (conn_failure_cnt_wwan + dropped) / (conn_failure_cnt_wwan + dropped + conn_success_cnt_wwan) * 100 AS failure_rate_wwan, (conn_failure_cnt_wimax + dropped) / (conn_failure_cnt_wimax + dropped + conn_success_cnt_wimax) * 100 AS failure_rate_wimax, (conn_failure_cnt_ethernet + dropped) / (conn_failure_cnt_ethernet + dropped + conn_success_cnt_ethernet) * 100 AS failure_rate_ethernet, c_date, hour\r\nfrom #HIVE_PARENT_TABLE\r\n', '1', '2011-06-20 23:23:12', '2011-07-15 18:45:54', '4', '', 'Daily Usage - Failure Rate - Child');
INSERT INTO `queries` VALUES ('53', '2', 'SELECT distinct error_code, error_msg, device_type, uuid, last_conn_timestamp, first_conn_timestamp, time_conn, c_date, sum_helper FROM (\r\nSELECT distinct error_code_wifi AS error_code, error_msg_wifi AS error_msg, device_type, uuid, last_conn_timestamp_wifi AS last_conn_timestamp, first_conn_timestamp_wifi AS first_conn_timestamp, time_conn_wifi AS time_conn, c_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\nSELECT distinct error_code_cdma AS error_code, error_msg_cdma AS error_msg, device_type, uuid, last_conn_timestamp_cdma AS last_conn_timestamp, first_conn_timestamp_cdma AS first_conn_timestamp, time_conn_cdma AS time_conn, c_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\nSELECT distinct error_code_wimax AS error_code, error_msg_wimax AS error_msg, device_type, uuid, last_conn_timestamp_wimax AS last_conn_timestamp, first_conn_timestamp_wimax AS first_conn_timestamp, time_conn_wimax AS time_conn, c_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\nSELECT distinct error_code_ethernet AS error_code, error_msg_ethernet AS error_msg, device_type, uuid, last_conn_timestamp_ethernet AS last_conn_timestamp, first_conn_timestamp_ethernet AS first_conn_timestamp, time_conn_ethernet AS time_conn, c_date, 1 AS sum_helper\r\nFROM #HIVE_PARENT_TABLE\r\n) a', '0', '2011-06-21 23:26:47', '2011-07-20 01:28:20', '19', '', 'Error Scatterplot - Child 1');
INSERT INTO `queries` VALUES ('54', '2', 'SELECT a.error_code AS error_code, a.error_msg AS error_msg, a.device_type AS device_type, (a.total_active_user_affected * 100 / b.total_active_user) AS percent_user_affected, a.frequency_per_hour AS frequency_per_hour FROM\r\n(SELECT error_code, error_msg, device_type, COUNT(DISTINCT uuid) AS total_active_user_affected, (COUNT(error_code)/CASE SUM(time_conn) WHEN 0.0 THEN 1.0 ELSE SUM(time_conn) END) AS frequency_per_hour, sum_helper\r\nFROM #CHILD_1\r\nWHERE error_code != \'\'\r\nGROUP BY error_code, error_msg, device_type, sum_helper) a\r\nJOIN\r\n(SELECT COUNT(DISTINCT uuid) AS total_active_user, sum_helper\r\nFROM #CHILD_1\r\nGROUP BY sum_helper) b\r\nON (a.sum_helper = b.sum_helper)', '1', '2011-06-22 00:40:10', '2011-07-20 01:28:20', '20', '', 'Error Scatterplot - Child 2');
INSERT INTO `queries` VALUES ('55', '22', 'SELECT uuid, c_36 AS manufacturer, c_37 AS device_model, year(server_date) AS year, Month(server_date) AS month\nFROM LogEvents\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\nGROUP BY uuid, c_36, c_37, year(server_date), Month(server_date)', '0', '2011-06-22 08:09:28', '2011-07-06 00:13:25', '45', '', 'Licensing Report 2 - Returning Licenses - Parent');
INSERT INTO `queries` VALUES ('56', '22', 'SELECT COUNT(cur_uuid.uuid) AS cnt_returning, 1 AS sum_helper\nFROM\n(\nSELECT uuid\nFROM #HIVE_PARENT_TABLE\n{where_1_month_ago}\nGROUP BY uuid\n) cur_uuid\nJOIN\n(\nSELECT uuid\nFROM #HIVE_PARENT_TABLE\n{where_before_1_month_ago}\nGROUP BY uuid\n) pre_uuid\nON (cur_uuid.uuid = pre_uuid.uuid)\n', '1', '2011-06-22 08:09:28', '2011-07-06 00:13:25', '46', '', 'Licensing Report 2 - Returning Licenses - Child');
INSERT INTO `queries` VALUES ('57', '23', 'SELECT uuid, c_36 AS manufacturer, c_37 AS device_model, year(server_date) AS year, Month(server_date) AS month\nFROM LogEvents\nWHERE (en = \"WiFiDeviceInsertion\" OR en=\"3GDeviceInsertion\" OR en=\"4GDeviceInsertion\")\nGROUP BY uuid, c_36, c_37, year(server_date), Month(server_date)', '0', '2011-06-22 08:17:13', '2011-07-20 00:08:19', '72', '', 'Licensing Report 3 - Lost Licenses - Parent');
INSERT INTO `queries` VALUES ('58', '23', 'SELECT COUNT(uuid) AS cnt_previous_total, 1 AS sum_helper\nFROM #HIVE_PARENT_TABLE\n{where_1_month_ago}\n{and_manufacturer}\n{and_device_model}', '0', '2011-06-22 08:17:13', '2011-07-20 00:08:19', '73', '', 'Licensing Report 3 - Lost Licenses - Child 1');
INSERT INTO `queries` VALUES ('59', '23', 'SELECT COUNT(cur_uuid.uuid) AS cnt_returning, 1 AS sum_helper\nFROM\n(\nSELECT uuid\nFROM #HIVE_PARENT_TABLE\n{where_1_month_ago}\n{and_manufacturer}\n{and_device_model}\nGROUP BY uuid\n) cur_uuid\nJOIN\n(\nSELECT uuid\nFROM #HIVE_PARENT_TABLE\n{where_before_1_month_ago}\n{and_manufacturer}\n{and_device_model}\nGROUP BY uuid\n) pre_uuid\nON (cur_uuid.uuid = pre_uuid.uuid)', '0', '2011-06-22 08:18:16', '2011-07-20 00:08:19', '74', '', 'Licensing Report 3 - Lost Licenses - Child 2');
INSERT INTO `queries` VALUES ('60', '23', 'SELECT COUNT(uuid) AS cnt_previous_total, 1 AS sum_helper\nFROM #HIVE_PARENT_TABLE\n{where_2_months_ago}\n{and_manufacturer}\n{and_device_model}', '1', '2011-06-22 08:19:00', '2011-07-20 00:08:19', '75', '', 'Licensing Report 3 - Lost Licenses - Child 3');
INSERT INTO `queries` VALUES ('61', '17', 'SELECT distinct\r\ndevice_type,\r\nsum_conn_attempts,\r\nyear,\r\nmonth,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM (\r\n        SELECT \'WiFiDevice\' AS device_type,\r\n        SUM(conn_attempts_wifi) AS sum_conn_attempts,\r\n        year,\r\n        month,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n        WHERE conn_attempts_wifi >= \'0\'\r\n        GROUP BY device_type, year, month, heat_lat, heat_lng, heat_x, heat_y, heat_num\r\nUNION ALL\r\n        SELECT \'3GDevice\' AS device_type,\r\n        SUM(conn_attempts_cdma) AS sum_conn_attempts,\r\n        year,\r\n        month,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n        WHERE conn_attempts_cdma >= \'0\'\r\n        GROUP BY device_type, year, month, heat_lat, heat_lng, heat_x, heat_y, heat_num\r\nUNION ALL\r\n        SELECT \'4GDevice\' AS device_type,\r\n        SUM(conn_attempts_wimax) AS sum_conn_attempts,\r\n        year,\r\n        month,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n        WHERE conn_attempts_wimax >= \'0\'\r\n        GROUP BY device_type, year, month, heat_lat, heat_lng, heat_x, heat_y, heat_num\r\nUNION ALL\r\n        SELECT \'EthernetDevice\' AS device_type,\r\n        SUM(conn_attempts_ethernet) AS sum_conn_attempts,\r\n        year,\r\n        month,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n        WHERE conn_attempts_ethernet >= \'0\'\r\n        GROUP BY device_type, year, month, heat_lat, heat_lng, heat_x, heat_y, heat_num\r\n) a', '1', '2011-06-22 20:16:47', '2011-07-20 00:30:21', '32', '', 'Total Connections Heat Map - Child');
INSERT INTO `queries` VALUES ('62', '24', 'SELECT devicetype AS device_type, SUM(c_158_1) AS conn_attempts_wifi, SUM(c_158_2) AS conn_attempts_cdma, SUM(c_158_3) AS conn_attempts_wimax, SUM(c_158_4) AS conn_attempts_ethernet, SUM(c_141) AS total_data_uploaded, SUM(c_142) AS total_data_downloaded, AVG(c_171_1) AS avg_download_speed_wifi, AVG(c_171_2) AS avg_download_speed_cdma, AVG(c_171_3) AS avg_download_speed_wimax, AVG(c_171_4) AS avg_download_speed_ethernet, AVG(c_162_1) AS avg_signal_strength_percent_wifi, AVG(c_162_2) AS avg_signal_strength_percent_cdma, AVG(c_162_3) AS avg_signal_strength_percent_wimax, AVG(c_162_4) AS avg_signal_strength_percent_ethernet, Year(server_date) as year, Month(server_date) AS month\r\nFROM LogEvents\r\nGROUP BY devicetype, Year(server_date), Month(server_date)', '0', '2011-07-06 20:32:49', '2011-07-18 20:42:02', '47', '', 'KPI - Technology Data - Connects Per Month - Parent');
INSERT INTO `queries` VALUES ('63', '24', 'SELECT distinct device_type, conn_attempts, throughput_per_day, avg_download_speed, avg_signal_strength_percent, server_date FROM (\r\nSELECT device_type, conn_attempts_wifi AS conn_attempts, AVG(total_data_uploaded + total_data_downloaded) AS throughput_per_day, avg_download_speed_wifi AS avg_download_speed, avg_signal_strength_percent_wifi AS avg_signal_strength_percent, concat(year,\"-\",month) AS server_date\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY device_type, conn_attempts_wifi, avg_download_speed_wifi, avg_signal_strength_percent_wifi, concat(year,\"-\",month)\r\nUNION ALL\r\nSELECT device_type, conn_attempts_cdma AS conn_attempts, AVG(total_data_uploaded + total_data_downloaded) AS throughput_per_day, avg_download_speed_cdma AS avg_download_speed, avg_signal_strength_percent_cdma AS avg_signal_strength_percent, concat(year,\"-\",month) AS server_date\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY device_type, conn_attempts_cdma, avg_download_speed_cdma, avg_signal_strength_percent_cdma, concat(year,\"-\",month)\r\nUNION ALL\r\nSELECT device_type, conn_attempts_wimax AS conn_attempts, AVG(total_data_uploaded + total_data_downloaded) AS throughput_per_day, avg_download_speed_wimax AS avg_download_speed, avg_signal_strength_percent_wimax AS avg_signal_strength_percent, concat(year,\"-\",month) AS server_date\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY device_type, conn_attempts_wimax, avg_download_speed_wimax, avg_signal_strength_percent_wimax, concat(year,\"-\",month)\r\nUNION ALL\r\nSELECT device_type, conn_attempts_ethernet AS conn_attempts, AVG(total_data_uploaded + total_data_downloaded) AS throughput_per_day, avg_download_speed_ethernet AS avg_download_speed, avg_signal_strength_percent_ethernet AS avg_signal_strength_percent, concat(year,\"-\",month) AS server_date\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY device_type, conn_attempts_ethernet, avg_download_speed_ethernet, avg_signal_strength_percent_ethernet, concat(year,\"-\",month)\r\n) a', '1', '2011-07-06 20:36:27', '2011-07-19 23:51:52', '48', '', 'KPI - Technology Data - Connects Per Month - Child');
INSERT INTO `queries` VALUES ('64', '25', 'SELECT devicetype AS device_type,\r\nSUM(c_158_1) AS conn_attempts_wifi,\r\nSUM(c_158_2) AS conn_attempts_cdma,\r\nSUM(c_158_3) AS conn_attempts_wimax,\r\nSUM(c_158_4) AS conn_attempts_ethernet,\r\nSUM(c_21) AS failed_conn_per_day_wifi,\r\nSUM(c_22) AS failed_conn_per_day_cdma,\r\nSUM(c_25) AS failed_conn_per_day_wimax,\r\nSUM(c_150) AS dropped_per_day,\r\nSUM(c_141) AS total_data_uploaded,\r\nSUM(c_142) AS total_data_downloaded,\r\nAVG(c_171_1) AS avg_download_speed_wifi,\r\nAVG(c_171_2) AS avg_download_speed_cdma,\r\nAVG(c_171_3) AS avg_download_speed_wimax,\r\nAVG(c_171_4) AS avg_download_speed_ethernet,\r\nAVG(c_162_1) AS avg_signal_strength_percent_wifi,\r\nAVG(c_162_2) AS avg_signal_strength_percent_cdma,\r\nAVG(c_162_3) AS avg_signal_strength_percent_wimax,\r\nAVG(c_162_4) AS avg_signal_strength_percent_ethernet,\r\nto_date(server_date) c_date\r\nFROM LogEvents\r\nGROUP BY devicetype, to_date(server_date)', '0', '2011-07-06 20:37:53', '2011-07-18 20:38:31', '49', '', 'KPI - Technology Data - Connects Per Day - Parent');
INSERT INTO `queries` VALUES ('65', '25', 'SELECT distinct\r\ndevice_type,\r\nconn_attempts,\r\nfailed_conn_per_day,\r\ndropped_per_day,\r\ndata_uploaded_per_day,\r\ndata_downloaded_per_day,\r\nthroughput_per_day,\r\navg_download_speed,\r\navg_signal_strength_percent,\r\nc_date\r\nFROM (\r\n        SELECT device_type,\r\n        conn_attempts_wifi AS conn_attempts,\r\n        failed_conn_per_day_wifi as failed_conn_per_day,\r\n        dropped_per_day as dropped_per_day,\r\n        total_data_uploaded AS data_uploaded_per_day,\r\n        total_data_downloaded AS data_downloaded_per_day,\r\n        AVG(total_data_uploaded + total_data_downloaded) AS throughput_per_day,\r\n        avg_download_speed_wifi AS avg_download_speed,\r\n        avg_signal_strength_percent_wifi AS avg_signal_strength_percent,\r\n        c_date\r\n        FROM #HIVE_PARENT_TABLE\r\n        GROUP BY device_type, conn_attempts_wifi, failed_conn_per_day_wifi, dropped_per_day, total_data_uploaded, total_data_downloaded, avg_download_speed_wifi, avg_signal_strength_percent_wifi, c_date\r\nUNION ALL\r\n        SELECT device_type,\r\n        conn_attempts_cdma AS conn_attempts,\r\n        failed_conn_per_day_cdma AS failed_conn_per_day,\r\n        cast(0 AS BIGINT) AS dropped_per_day,\r\n        cast(0 AS BIGINT) AS data_uploaded_per_day,\r\n        cast(0 AS BIGINT) AS data_downloaded_per_day,\r\n        AVG(total_data_uploaded + total_data_downloaded) AS throughput_per_day,\r\n        avg_download_speed_cdma AS avg_download_speed,\r\n        avg_signal_strength_percent_cdma AS avg_signal_strength_percent,\r\n        c_date\r\n        FROM #HIVE_PARENT_TABLE\r\n        GROUP BY device_type, conn_attempts_cdma, failed_conn_per_day_cdma, avg_download_speed_cdma, avg_signal_strength_percent_cdma, c_date\r\nUNION ALL\r\n        SELECT device_type,\r\n        conn_attempts_wimax AS conn_attempts,\r\n        failed_conn_per_day_wimax AS failed_conn_per_day,\r\n        cast(0 AS BIGINT) AS dropped_per_day,\r\n        cast(0 AS BIGINT) AS data_uploaded_per_day,\r\n        cast(0 AS BIGINT) AS data_downloaded_per_day,\r\n        AVG(total_data_uploaded + total_data_downloaded) AS throughput_per_day,\r\n        avg_download_speed_wimax AS avg_download_speed,\r\n        avg_signal_strength_percent_wimax AS avg_signal_strength_percent,\r\n        c_date\r\n        FROM #HIVE_PARENT_TABLE\r\n        GROUP BY device_type, conn_attempts_wimax, failed_conn_per_day_wimax, avg_download_speed_wimax, avg_signal_strength_percent_wimax, c_date\r\nUNION ALL\r\n        SELECT device_type,\r\n        conn_attempts_ethernet AS conn_attempts,\r\n        cast(0 AS BIGINT) AS failed_conn_per_day,\r\n        cast(0 AS BIGINT) AS dropped_per_day,\r\n        cast(0 AS BIGINT) AS data_uploaded_per_day,\r\n        cast(0 AS BIGINT) AS data_downloaded_per_day,\r\n        AVG(total_data_uploaded + total_data_downloaded) AS throughput_per_day,\r\n        avg_download_speed_ethernet AS avg_download_speed,\r\n        avg_signal_strength_percent_ethernet AS avg_signal_strength_percent,\r\n        c_date\r\n        FROM #HIVE_PARENT_TABLE\r\n        GROUP BY device_type, conn_attempts_ethernet, avg_download_speed_ethernet, avg_signal_strength_percent_ethernet, c_date\r\n) a', '1', '2011-07-06 20:40:41', '2011-07-20 01:09:18', '50', '', 'KPI - Technology Data - Connects Per Day - Child');
INSERT INTO `queries` VALUES ('66', '26', 'SELECT devicetype AS device_type,\r\nSUM(c_158_1) AS conn_attempts_wifi,\r\nSUM(c_158_2) AS conn_attempts_cdma,\r\nSUM(c_158_3) AS conn_attempts_wimax,\r\nSUM(c_158_4) AS conn_attempts_ethernet,\r\nto_date(server_date) AS c_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM LogEvents\r\nGROUP BY devicetype, to_date(server_date), heat_lat, heat_lng, heat_x, heat_y, heat_num', '0', '2011-07-06 21:17:38', '2011-07-22 01:23:41', '51', '', 'Connections per Day Heat Map - Parent');
INSERT INTO `queries` VALUES ('67', '27', 'SELECT devicetype AS device_type,\r\nSUM(c_141) AS total_data_uploaded,\r\nSUM(c_142) AS total_data_downloaded,\r\nto_date(server_date) AS c_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM LogEvents\r\nGROUP BY devicetype, to_date(server_date), heat_lat, heat_lng, heat_x, heat_y, heat_num', '0', '2011-07-06 22:28:34', '2011-07-22 01:54:47', '60', '', 'Total Throughput Heat Map - Parent');
INSERT INTO `queries` VALUES ('68', '28', 'SELECT devicetype AS device_type,\r\nSUM(c_141) AS total_data_uploaded,\r\nSUM(c_142) AS total_data_downloaded,\r\nto_date(server_date) AS c_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM LogEvents\r\nGROUP BY devicetype, to_date(server_date), heat_lat, heat_lng, heat_x, heat_y, heat_num', '0', '2011-07-06 22:44:42', '2011-07-22 02:05:48', '61', '', 'Throughput per Day Heat Map - Parent');
INSERT INTO `queries` VALUES ('69', '29', 'SELECT * FROM SOME_TABLE', '1', '2011-07-06 22:50:34', '2011-07-06 22:50:34', '62', '', 'Average Signal Strength Heat Map');
INSERT INTO `queries` VALUES ('70', '30', 'SELECT * FROM SOME_TABLE', '1', '2011-07-06 22:55:20', '2011-07-06 22:55:20', '63', '', 'Average Peak Download Speed Heat Map');
INSERT INTO `queries` VALUES ('71', '31', 'SELECT distinct\r\ndevicetype,\r\nc_160_1 AS error_code_wifi,\r\nc_160_2 AS error_code_cdma,\r\nc_160_3 AS error_code_wimax,\r\nc_160_4 AS error_code_ethernet,\r\nc_170_1 AS time_conn_wifi,\r\nc_170_2 AS time_conn_cdma,\r\nc_170_3 AS time_conn_wimax,\r\nc_170_4 AS time_conn_ethernet,\r\nto_date(server_date) AS c_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM LogEvents', '0', '2011-07-06 23:00:03', '2011-08-05 16:13:41', '64', '', 'Error Frequency Heat Map - Parent');
INSERT INTO `queries` VALUES ('72', '32', 'SELECT * FROM SOME_TABLE', '1', '2011-07-06 23:04:15', '2011-07-06 23:04:15', '65', '', 'Connection Failure Rate Heat Map');
INSERT INTO `queries` VALUES ('73', '14', 'SELECT distinct a.ds, a.manufacturer, a.device_model, b.data_download_per_day, b.data_upload_per_day, time_conn_per_day\r\nFROM\r\n        (SELECT distinct uuid, ds, manufacturer, device_model FROM #HIVE_PARENT_TABLE) a\r\nJOIN\r\n        (\r\n        SELECT uuid, ds, SUM(data_download_per_day) AS data_download_per_day, SUM(data_upload_per_day) AS data_upload_per_day, SUM(time_conn_per_day) AS time_conn_per_day FROM (\r\n                SELECT uuid, ds, SUM(data_download_per_day) AS data_download_per_day, SUM(data_upload_per_day) AS data_upload_per_day, SUM(time_conn_per_day_wifi) AS time_conn_per_day FROM #TABLE_PREVIOUS GROUP BY uuid, ds\r\n                UNION ALL\r\n                SELECT uuid, ds, SUM(data_download_per_day) AS data_download_per_day, SUM(data_upload_per_day) AS data_upload_per_day, SUM(time_conn_per_day_cdma) AS time_conn_per_day FROM #TABLE_PREVIOUS GROUP BY uuid, ds\r\n                UNION ALL\r\n                SELECT uuid, ds, SUM(data_download_per_day) AS data_download_per_day, SUM(data_upload_per_day) AS data_upload_per_day, SUM(time_conn_per_day_wimax) AS time_conn_per_day FROM #TABLE_PREVIOUS GROUP BY uuid, ds\r\n                UNION ALL\r\n                SELECT uuid, ds, SUM(data_download_per_day) AS data_download_per_day, SUM(data_upload_per_day) AS data_upload_per_day, SUM(time_conn_per_day_ethernet) AS time_conn_per_day FROM #TABLE_PREVIOUS GROUP BY uuid, ds\r\n        ) u1 GROUP BY uuid, ds\r\n        ) b\r\nON (a.uuid=b.uuid AND a.ds=b.ds)', '0', '2011-07-06 23:19:24', '2011-07-18 20:14:55', '66', '', 'Device Usage Summary - Child 2');
INSERT INTO `queries` VALUES ('74', '14', 'SELECT a.ds, a.cnt_unique_uuid, b.total_unique_uuid, a.manufacturer, a.device_model\r\nFROM (\r\n        SELECT COUNT(distinct uuid) AS cnt_unique_uuid, manufacturer, device_model, ds, 1 AS sum_helper FROM #HIVE_PARENT_TABLE group by manufacturer, device_model, ds\r\n        ) a\r\nJOIN (\r\n        SELECT COUNT(distinct uuid) AS total_unique_uuid, 1 AS sum_helper FROM #HIVE_PARENT_TABLE\r\n        ) b\r\nON (a.sum_helper=b.sum_helper)', '0', '2011-07-06 23:34:42', '2011-07-19 18:39:45', '67', '', 'Device Usage Summary - Child 3');
INSERT INTO `queries` VALUES ('75', '14', 'SELECT distinct a.ds, a.manufacturer, a.device_model, a.data_download_per_day, a.data_upload_per_day, (b.cnt_unique_uuid * 100 / b.total_unique_uuid) AS share, a.time_conn_per_day\r\nFROM\r\n        (SELECT ds, manufacturer, device_model, SUM(data_download_per_day) AS data_download_per_day, SUM(data_upload_per_day) AS data_upload_per_day, SUM(time_conn_per_day) AS time_conn_per_day FROM #CHILD_2 GROUP BY ds, manufacturer, device_model) a\r\nJOIN\r\n        (SELECT distinct ds, cnt_unique_uuid, total_unique_uuid, manufacturer, device_model FROM #TABLE_PREVIOUS) b\r\nON (a.ds=b.ds AND a.manufacturer=b.manufacturer AND a.device_model=b.device_model)', '1', '2011-07-06 23:37:53', '2011-07-19 18:39:45', '68', '', 'Device Usage Summary - Child 4');
INSERT INTO `queries` VALUES ('76', '13', 'SELECT distinct a.ds, b.device_type, a.manufacturer, a.device_model, b.data_download_per_day, b.data_upload_per_day, b.time_conn_per_day, b.avg_peek_download_speed, b.avg_peek_upload_speed\r\nFROM\r\n        (SELECT distinct uuid, ds, manufacturer, device_model from #HIVE_PARENT_TABLE) a\r\nJOIN\r\n        (\r\n        SELECT uuid, ds, device_type, SUM(data_download_per_day) AS data_download_per_day, SUM(data_upload_per_day) AS data_upload_per_day, SUM(time_conn_per_day) AS time_conn_per_day, AVG(avg_peek_download_speed) AS avg_peek_download_speed, AVG(avg_peek_upload_speed) AS avg_peek_upload_speed FROM (\r\n                SELECT\r\n                        uuid, ds, device_type,\r\n                        SUM(data_download_per_day) AS data_download_per_day,\r\n                        SUM(data_upload_per_day) AS data_upload_per_day,\r\n                        SUM(time_conn_per_day_wifi) AS time_conn_per_day,\r\n                        AVG(avg_peek_download_speed_wifi) AS avg_peek_download_speed,\r\n                        AVG(avg_peek_upload_speed_wifi) AS avg_peek_upload_speed\r\n                        FROM #TABLE_PREVIOUS GROUP BY uuid, ds, device_type\r\n                UNION ALL\r\n                SELECT\r\n                        uuid, ds, device_type,\r\n                        SUM(data_download_per_day) AS data_download_per_day,\r\n                        SUM(data_upload_per_day) AS data_upload_per_day,\r\n                        SUM(time_conn_per_day_cdma) AS time_conn_per_day,\r\n                        AVG(avg_peek_download_speed_cdma) AS avg_peek_download_speed,\r\n                        AVG(avg_peek_upload_speed_cdma) AS avg_peek_upload_speed\r\n                        FROM #TABLE_PREVIOUS GROUP BY uuid, ds, device_type\r\n                UNION ALL\r\n                SELECT\r\n                        uuid, ds, device_type,\r\n                        SUM(data_download_per_day) AS data_download_per_day,\r\n                        SUM(data_upload_per_day) AS data_upload_per_day,\r\n                        SUM(time_conn_per_day_wimax) AS time_conn_per_day,\r\n                        AVG(avg_peek_download_speed_wimax) AS avg_peek_download_speed,\r\n                        AVG(avg_peek_upload_speed_wimax) AS avg_peek_upload_speed\r\n                        FROM #TABLE_PREVIOUS GROUP BY uuid, ds, device_type\r\n                UNION ALL\r\n                SELECT\r\n                        uuid, ds, device_type,\r\n                        SUM(data_download_per_day) AS data_download_per_day,\r\n                        SUM(data_upload_per_day) AS data_upload_per_day,\r\n                        SUM(time_conn_per_day_ethernet) AS time_conn_per_day,\r\n                        AVG(avg_peek_download_speed_ethernet) AS avg_peek_download_speed,\r\n                        AVG(avg_peek_upload_speed_ethernet) AS avg_peek_upload_speed\r\n                        FROM #TABLE_PREVIOUS GROUP BY uuid, ds, device_type\r\n        ) u1 GROUP BY uuid, ds, device_type\r\n        ) b\r\nON (a.uuid=b.uuid AND a.ds=b.ds)', '0', '2011-07-07 06:51:13', '2011-07-20 01:35:04', '69', '', 'Device Usage Report - Child 2');
INSERT INTO `queries` VALUES ('77', '13', 'SELECT a.ds, a.cnt_unique_uuid, b.total_unique_uuid, a.manufacturer, a.device_model\r\nFROM (\r\n        SELECT COUNT(distinct uuid) AS cnt_unique_uuid, manufacturer, device_model, ds, 1 AS sum_helper FROM #HIVE_PARENT_TABLE GROUP BY manufacturer, device_model, ds\r\n        ) a\r\nJOIN (\r\n        SELECT COUNT(distinct uuid) AS total_unique_uuid, 1 AS sum_helper FROM #HIVE_PARENT_TABLE\r\n        ) b\r\nON (a.sum_helper=b.sum_helper)', '0', '2011-07-07 06:53:24', '2011-07-20 01:35:04', '70', '', 'Device Usage Report - Child 3');
INSERT INTO `queries` VALUES ('78', '13', 'SELECT a.ds, a.device_type, a.manufacturer, a.device_model, a.data_download_per_day, a.data_upload_per_day, (b.cnt_unique_uuid * 100 / b.total_unique_uuid) AS share, b.cnt_unique_uuid AS device_user, a.time_conn_per_day, a.avg_peek_download_speed, a.avg_peek_upload_speed\r\nFROM\r\n        (SELECT\r\n                ds, device_type, manufacturer, device_model,\r\n                SUM(data_download_per_day) AS data_download_per_day,\r\n                SUM(data_upload_per_day) AS data_upload_per_day,\r\n                SUM(time_conn_per_day) AS time_conn_per_day,\r\n                AVG(avg_peek_download_speed) AS avg_peek_download_speed,\r\n                AVG(avg_peek_upload_speed) AS avg_peek_upload_speed\r\n                FROM #CHILD_2 GROUP BY ds, device_type, manufacturer, device_model) a\r\nJOIN\r\n        (SELECT distinct ds, cnt_unique_uuid, total_unique_uuid, manufacturer, device_model FROM #CHILD_3) b\r\nON (a.ds=b.ds AND a.manufacturer=b.manufacturer AND a.device_model=b.device_model)', '1', '2011-07-07 06:54:37', '2011-07-20 01:35:04', '71', '', 'Device Usage Report - Child 4');
INSERT INTO `queries` VALUES ('79', '33', 'SELECT distinct manufacturer, device_model\r\nFROM login_summary', '1', '2011-07-21 07:10:16', '2011-07-21 07:10:17', '76', '', 'All Devices');
INSERT INTO `queries` VALUES ('80', '26', 'SELECT distinct\r\ndevice_type,\r\nsum_conn_attempts,\r\nc_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM (\r\n        SELECT \'WiFiDevice\' AS device_type,\r\n        SUM(conn_attempts_wifi) AS sum_conn_attempts,\r\n        c_date,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n        WHERE conn_attempts_wifi >= \'0\'\r\n        GROUP BY device_type, c_date, heat_lat, heat_lng, heat_x, heat_y, heat_num\r\nUNION ALL\r\n        SELECT \'3GDevice\' AS device_type,\r\n        SUM(conn_attempts_cdma) AS sum_conn_attempts,\r\n        c_date,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n        WHERE conn_attempts_cdma >= \'0\'\r\n        GROUP BY device_type, c_date, heat_lat, heat_lng, heat_x, heat_y, heat_num\r\nUNION ALL\r\n        SELECT \'4GDevice\' AS device_type,\r\n        SUM(conn_attempts_wimax) AS sum_conn_attempts,\r\n        c_date,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n        WHERE conn_attempts_wimax >= \'0\'\r\n        GROUP BY device_type, c_date, heat_lat, heat_lng, heat_x, heat_y, heat_num\r\nUNION ALL\r\n        SELECT \'EthernetDevice\' AS device_type,\r\n        SUM(conn_attempts_ethernet) AS sum_conn_attempts,\r\n        c_date,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n        WHERE conn_attempts_ethernet >= \'0\'\r\n        GROUP BY device_type, c_date, heat_lat, heat_lng, heat_x, heat_y, heat_num\r\n) a', '1', '2011-07-22 01:23:41', '2011-07-22 01:31:43', '77', '', 'Connections per Day Heat Map - Child');
INSERT INTO `queries` VALUES ('81', '28', 'SELECT device_type,\r\nAVG(total_data_uploaded + total_data_downloaded) AS throughput_sum_per_day,\r\nc_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY device_type, (total_data_uploaded + total_data_downloaded), c_date, heat_lat, heat_lng, heat_x, heat_y, heat_num', '1', '2011-07-22 01:26:29', '2011-07-22 01:57:39', '78', '', 'Throughput per Day Heat Map - Child');
INSERT INTO `queries` VALUES ('82', '27', 'SELECT device_type,\r\n(total_data_uploaded + total_data_downloaded) AS throughput_sum_per_day,\r\nc_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM #HIVE_PARENT_TABLE\r\nGROUP BY device_type, (total_data_uploaded + total_data_downloaded), c_date, heat_lat, heat_lng, heat_x, heat_y, heat_num', '1', '2011-07-22 01:54:47', '2011-07-22 01:54:47', '79', '', 'Total Throughput Heat Map - Child');
INSERT INTO `queries` VALUES ('83', '31', 'SELECT distinct\r\ndevicetype,\r\nerror_code,\r\ntime_conn,\r\nc_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM (\r\n        SELECT distinct\r\n        devicetype,\r\n        error_code_wifi AS error_code,\r\n        time_conn_wifi AS time_conn,\r\n        c_date,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\n        SELECT distinct\r\n        devicetype,\r\n        error_code_cdma AS error_code,\r\n        time_conn_cdma AS time_conn,\r\n        c_date,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\n        SELECT distinct\r\n        devicetype,\r\n        error_code_wimax AS error_code,\r\n        time_conn_wimax AS time_conn,\r\n        c_date,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\nUNION ALL\r\n        SELECT distinct\r\n        devicetype,\r\n        error_code_ethernet AS error_code,\r\n        time_conn_ethernet AS time_conn,\r\n        c_date,\r\n        heat_lat,\r\n        heat_lng,\r\n        heat_x,\r\n        heat_y,\r\n        heat_num\r\n        FROM #HIVE_PARENT_TABLE\r\n) a', '0', '2011-08-05 16:13:41', '2011-08-05 16:13:59', '80', '', 'Error Frequency Heat Map - Child 1');
INSERT INTO `queries` VALUES ('84', '31', 'SELECT devicetype,\r\nerror_code,\r\n(COUNT(error_code)/CASE SUM(time_conn) WHEN 0.0 THEN 1.0 ELSE SUM(time_conn) END) AS frequency_per_hour,\r\nc_date,\r\nheat_lat,\r\nheat_lng,\r\nheat_x,\r\nheat_y,\r\nheat_num\r\nFROM #PREVIOUS\r\nWHERE error_code != \'\'\r\nGROUP BY devicetype, error_code, c_date, heat_lat, heat_lng, heat_x, heat_y, heat_num', '1', '2011-08-05 16:14:23', '2011-08-05 16:14:23', '81', '', 'Error Frequency Heat Map - Child 2');
INSERT INTO `report_links` VALUES ('1', 'map', '8', '22', '', '2011-07-07 01:41:17', '2011-07-07 01:41:17');
INSERT INTO `report_links` VALUES ('2', 'profile', '8', '11', '', '2011-07-07 01:41:17', '2011-07-07 01:41:17');
INSERT INTO `report_links` VALUES ('3', 'trend', '11', '3', '', '2011-07-07 01:42:56', '2011-07-07 01:42:56');
INSERT INTO `report_links` VALUES ('4', 'tod', '9', '5', '', '2011-07-07 01:44:36', '2011-07-07 01:44:36');
INSERT INTO `report_links` VALUES ('5', 'dow', '9', '1', '', '2011-07-07 01:44:36', '2011-07-07 01:44:36');
INSERT INTO `report_links` VALUES ('6', 'trend', '9', '3', '', '2011-07-07 01:44:36', '2011-07-07 01:44:36');
INSERT INTO `report_queries` VALUES ('1', '1', '13', '2011-05-27 00:51:12', '2011-05-29 20:28:10', 'line', '{\n\"color\": \"#21415E\"\n}', '', 'connections_per_day', '', '', '', 'SELECT day_of_weeks.name, (SUM(h.conn_per_day_wifi) + SUM(h.conn_per_day_cdma) + SUM(h.conn_per_day_wimax) + SUM(h.conn_per_day_ethernet)) as connections_per_day\nFROM day_of_weeks\nLEFT JOIN {table_name} AS h\nON day_of_weeks.name = h.day_of_week\nGROUP BY day_of_weeks.name\nORDER BY day_of_weeks.id', '0');
INSERT INTO `report_queries` VALUES ('2', '1', '22', '2011-05-27 00:51:12', '2011-05-29 20:28:10', 'area', '{\n\"color\": \"#8DAF17\"\n}', '', 'sum_avg_upload_download', '', '', '', 'SELECT day_of_weeks.name, h.sum_avg_upload_download AS sum_avg_upload_download \nFROM day_of_weeks\nLEFT JOIN {table_name} AS h\nON day_of_weeks.name = h.day_of_week\nGROUP BY day_of_weeks.name\nORDER BY day_of_weeks.id', '1');
INSERT INTO `report_queries` VALUES ('4', '2', '54', '2011-05-27 00:51:12', '2011-07-21 00:30:23', 'scatter', '{\n\"color\": \"#F89A2D\"\n}', 'frequency_per_hour', 'percent_user_affected', 'percent_user_affected', 'description', '', 'SELECT CONCAT_WS(\', \', error_code, error_msg, device_type, \'<br/>\') AS description, error_code, percent_user_affected, frequency_per_hour FROM {table_name}', '0');
INSERT INTO `report_queries` VALUES ('6', '3', '34', '2011-05-27 00:51:12', '2011-07-21 00:41:39', 'line', '{\n\"color\": \"#21415E\"\n}', 'c_date', 'conn_per_day', '', '', '', 'SELECT (SUM(conn_per_day_wifi) + SUM(conn_per_day_cdma) + SUM(conn_per_day_wimax) + SUM(conn_per_day_ethernet)) AS conn_per_day, device_type, manufacturer, device_model, c_date\nFROM {table_name}\n{where_filter}\nGROUP BY c_date\n', '0');
INSERT INTO `report_queries` VALUES ('8', '4', '18', '2011-05-27 00:51:12', '2011-07-21 00:32:47', 'area', '', 'server_date', 'sum_avg_upload_download_3g', '', '', '', 'SELECT sum_avg_upload_download AS sum_avg_upload_download_3g, server_date FROM {table_name} WHERE device_type = \"3GDevice\"\n{and_filter};', '0');
INSERT INTO `report_queries` VALUES ('9', '4', '18', '2011-05-27 00:51:12', '2011-07-21 00:32:47', 'area', '', 'server_date', 'sum_avg_upload_download_ethernet', '', '', '', 'SELECT sum_avg_upload_download AS sum_avg_upload_download_ethernet, server_date FROM {table_name} WHERE device_type = \"EthernetDevice\"\n{and_filter};', '0');
INSERT INTO `report_queries` VALUES ('10', '5', '5', '2011-05-27 00:51:12', '2011-06-28 16:34:56', 'line', '{\n\"color\": \"#21415E\"\n}\n', 'hour', 'connections_per_hour', '', '', '', 'SELECT (conn_per_hour_wifi + conn_per_hour_cdma + conn_per_hour_wimax + conn_per_hour_ethernet) AS connections_per_hour, hour\nFROM {table_name}\nGROUP BY hour;', '0');
INSERT INTO `report_queries` VALUES ('11', '5', '24', '2011-05-27 00:51:12', '2011-06-28 16:35:29', 'area', '{\n\"color\": \"#8DAF17\"\n}', 'hour', 'sum_avg_upload_download', '', '', '', 'SELECT sum_avg_upload_download, hour\nFROM {table_name}\nGROUP BY hour;', '1');
INSERT INTO `report_queries` VALUES ('17', '7', '25', '2011-05-27 00:51:12', '2011-07-21 00:36:07', 'pie', '', 'device_model', 'device_usage_percentage', '', '', '', 'SELECT (SUM(percent_uuid) * 100) as device_usage_percentage, device_model FROM {table_name} WHERE device_model != \'\' GROUP BY device_model;', '0');
INSERT INTO `report_queries` VALUES ('18', '8', '38', '2011-05-27 00:51:12', '2011-07-21 00:30:05', null, null, null, null, null, null, '', 'SELECT error_code, error_msg, device_type AS connection_type, SUM(percent_user_affected) AS percent_user_affected, SUM(frequency_per_hour) AS frequency_per_hour, SUM(total_cnt_error) AS total_cnt_error\nFROM {table_name}\n{where_filter}\nGROUP BY error_code, device_type', '0');
INSERT INTO `report_queries` VALUES ('19', '9', '27', '2011-05-28 07:01:42', '2011-05-29 00:50:51', null, null, null, null, null, null, '', 'SELECT connection_type, SUM(conn_per_day_wifi) AS conn_per_day, SUM(failed_conn_per_day_wifi) AS failed_conn_per_day, SUM(dropped_per_day) as dropped_per_day, AVG(avg_time_conn_per_day_wifi) AS avg_time_conn_per_day, AVG(avg_time_conn_per_session_wifi) AS avg_time_conn_per_session, AVG(avg_downloaded_per_day) AS avg_downloaded_per_day, AVG(avg_uploaded_per_day) AS avg_uploaded_per_day, AVG(avg_download_speed_wifi) AS avg_download_speed, AVG(avg_upload_speed_wifi) AS avg_upload_speed, AVG(avg_signal_strength_wifi) AS avg_signal_strength FROM {table_name} WHERE connection_type = \'WiFiDevice\' GROUP BY connection_type', '0');
INSERT INTO `report_queries` VALUES ('20', '9', '27', '2011-05-28 20:33:55', '2011-05-29 00:50:51', null, null, null, null, null, null, '', 'SELECT connection_type, SUM(conn_per_day_ethernet) AS conn_per_day, SUM(dropped_per_day) AS dropped_per_day, AVG(avg_time_conn_per_day_ethernet) AS avg_time_conn_per_day, AVG(avg_time_conn_per_session_ethernet) AS avg_time_conn_per_session, AVG(avg_downloaded_per_day) AS avg_downloaded_per_day, avg_uploaded_per_day, AVG(avg_download_speed_ethernet) AS avg_download_speed, AVG(avg_upload_speed_ethernet) AS avg_upload_speed, AVG(avg_signal_strength_ethernet) AS avg_signal_strength FROM {table_name} WHERE connection_type = \'EthernetDevice\' GROUP BY connection_type', '0');
INSERT INTO `report_queries` VALUES ('21', '9', '27', '2011-05-28 20:36:35', '2011-05-29 00:51:27', null, null, null, null, null, null, '', 'SELECT connection_type, SUM(conn_per_day_cdma) AS conn_per_day, SUM(failed_conn_per_day_wwan) AS failed_conn_per_day, SUM(dropped_per_day) AS dropped_per_day, AVG(avg_time_conn_per_day_cdma) AS avg_time_conn_per_day, AVG(avg_time_conn_per_session_cdma) AS avg_time_conn_per_session, AVG(avg_downloaded_per_day) AS avg_downloaded_per_day, AVG(avg_uploaded_per_day) AS avg_uploaded_per_day, AVG(avg_download_speed_cdma) AS avg_download_speed, AVG(avg_upload_speed_cdma) AS avg_upload_speed, AVG(avg_signal_strength_cdma) AS avg_signal_strength FROM {table_name} WHERE connection_type = \'3GDevice\' GROUP BY connection_type', '0');
INSERT INTO `report_queries` VALUES ('22', '9', '27', '2011-05-28 20:37:25', '2011-05-29 00:50:51', null, null, null, null, null, null, '', 'SELECT connection_type, SUM(conn_per_day_wimax) AS conn_per_day, SUM(failed_conn_per_day_wimax) AS failed_conn_per_day, SUM(dropped_per_day) AS dropped_per_day, AVG(avg_time_conn_per_day_wimax) AS avg_time_conn_per_day, AVG(avg_time_conn_per_session_wimax) AS avg_time_conn_per_session, AVG(avg_downloaded_per_day) AS avg_downloaded_per_day, AVG(avg_uploaded_per_day) AS avg_uploaded_per_day, AVG(avg_download_speed_wimax) AS avg_download_speed, AVG(avg_upload_speed_wimax) AS avg_upload_speed, AVG(avg_signal_strength_wimax) AS avg_signal_strength FROM {table_name} WHERE connection_type = \'4GDevice\' GROUP BY connection_type', '0');
INSERT INTO `report_queries` VALUES ('23', '10', '29', '2011-05-28 21:19:00', '2011-07-21 00:31:50', null, null, null, null, null, null, '', 'SELECT device_type as connection_type, SUM(conn_attempts_wifi) AS conn_per_day, SUM(throughput_per_day) AS throughput_per_day, SUM(download_speed_wifi) AS avg_download_speed, AVG(signal_strength_percent_wifi) AS avg_signal_strength\nFROM {table_name}\nWHERE device_type = \'WiFiDevice\'\nGROUP BY device_type', '0');
INSERT INTO `report_queries` VALUES ('24', '10', '29', '2011-05-28 21:30:53', '2011-07-21 00:31:50', null, null, null, null, null, null, '', 'SELECT device_type as connection_type, SUM(conn_attempts_ethernet) AS conn_per_day, SUM(throughput_per_day) AS throughput_per_day, AVG(download_speed_ethernet) AS avg_download_speed, AVG(signal_strength_percent_ethernet) AS avg_signal_strength\nFROM {table_name}\nWHERE device_type = \'EthernetDevice\'\nGROUP BY device_type', '0');
INSERT INTO `report_queries` VALUES ('25', '10', '29', '2011-05-28 21:32:19', '2011-07-21 00:31:50', null, null, null, null, null, null, '', 'SELECT device_type as connection_type, SUM(conn_attempts_wimax) AS conn_per_day, SUM(throughput_per_day) AS throughput_per_day, AVG(download_speed_wimax) AS avg_download_speed, AVG(signal_strength_percent_wimax) AS avg_signal_strength\nFROM {table_name}\nWHERE device_type = \'4GDevice\'\nGROUP BY device_type', '0');
INSERT INTO `report_queries` VALUES ('26', '10', '29', '2011-05-28 21:34:07', '2011-07-21 00:31:50', null, null, null, null, null, null, '', 'SELECT device_type as connection_type, SUM(conn_attempts_cdma) AS conn_per_day, SUM(throughput_per_day) AS throughput_per_day, AVG(download_speed_cdma) AS avg_download_speed, AVG(signal_strength_percent_cdma) AS avg_signal_strength\nFROM {table_name}\nWHERE device_type = \'3GDevice\'\nGROUP BY device_type', '0');
INSERT INTO `report_queries` VALUES ('27', '11', '78', '2011-05-29 02:55:52', '2011-07-21 00:37:13', null, null, null, null, null, null, '', 'SELECT manufacturer, device_model, AVG(share) AS user_share, SUM(device_user) AS sum_users, AVG(time_conn_per_day) AS avg_conn_time_per_day, AVG(data_upload_per_day) AS avg_data_upload_per_day, AVG(data_download_per_day) AS avg_data_download_per_day, AVG(avg_peek_download_speed) AS avg_peak_download_speed, AVG(avg_peek_upload_speed) AS avg_peak_upload_speed\nFROM {table_name}\n{where_filter}\nGROUP BY manufacturer, device_model', '0');
INSERT INTO `report_queries` VALUES ('29', '13', '75', '2011-05-29 03:06:48', '2011-07-21 00:37:45', null, null, null, null, null, null, '', 'SELECT manufacturer, device_model, AVG(share) AS user_share, AVG(time_conn_per_day) AS avg_time_conn_per_day, AVG(data_download_per_day + data_upload_per_day) AS avg_throughput_per_day\nFROM {table_name}\nGROUP BY manufacturer, device_model', '0');
INSERT INTO `report_queries` VALUES ('30', '3', '36', '2011-05-29 03:58:20', '2011-07-21 00:38:35', 'area', '{\n\"color\": \"#8DAF17\"\n}', 'c_date', 'sum_avg_upload_download', '', '', '', 'SELECT SUM(sum_avg_upload_download) AS sum_avg_upload_download, device_type, manufacturer, device_model, c_date\nFROM {table_name}\n{where_filter}\nGROUP BY c_date\n', '1');
INSERT INTO `report_queries` VALUES ('31', '14', '41', '2011-05-29 06:40:57', '2011-05-29 06:40:57', null, null, null, null, null, null, '', 'SELECT error_code, error_msg, SUM(percent_user_affected) AS percent_user_affected, SUM(frequency_per_hour) AS frequency_per_hour\nFROM {table_name}\nGROUP BY error_code', '0');
INSERT INTO `report_queries` VALUES ('32', '4', '18', '2011-06-06 08:00:05', '2011-07-21 00:32:47', 'area', '', 'server_date', 'sum_avg_upload_download_wifi', '', '', '', 'SELECT sum_avg_upload_download AS sum_avg_upload_download_wifi, server_date FROM {table_name} WHERE device_type = \"WiFiDevice\"\n{and_filter};', '0');
INSERT INTO `report_queries` VALUES ('33', '4', '18', '2011-06-06 08:01:19', '2011-07-21 00:32:47', 'area', '', 'server_date', 'sum_avg_upload_download_4g', '', '', '', 'SELECT sum_avg_upload_download AS sum_avg_upload_download_4g, server_date FROM {table_name} WHERE device_type = \"4GDevice\"\n{and_filter};', '0');
INSERT INTO `report_queries` VALUES ('34', '15', '61', '2011-06-07 21:26:19', '2011-07-06 20:55:13', null, null, null, null, null, null, '', 'SELECT devicetype, heat_lat, heat_lng, heat_x, heat_y, heat_num, SUM(sum_conn_attempts) AS sum_conn_attempts\nFROM {table_name}\nGROUP BY heat_lat, heat_lng, heat_x, heat_y, heat_num\n{and_filter}', '0');
INSERT INTO `report_queries` VALUES ('35', '16', '44', '2011-06-14 10:26:45', '2011-09-01 00:45:08', null, null, null, null, null, null, '', 'SELECT SUM(cnt_current_total) AS cnt_current_total, server_date\r\nFROM {table_name}\r\nGROUP BY server_date\r\nORDER BY server_date ASC', '0');
INSERT INTO `report_queries` VALUES ('36', '5', '52', '2011-06-27 23:19:40', '2011-07-07 01:06:27', 'line', '{\n\"color\": \"red\"\n}', 'hour', 'failure_rate', '', '', '', 'SELECT (failure_rate_wifi + failure_rate_wwan + failure_rate_wimax + failure_rate_ethernet) AS failure_rate, hour\nFROM {table_name}\nGROUP BY hour;', '2');
INSERT INTO `report_queries` VALUES ('37', '3', '48', '2011-06-27 23:44:01', '2011-07-21 00:38:35', 'line', '{\n\"color\": \"red\"\n}', 'c_date', 'failure_rate', '', '', '', 'SELECT (failure_rate_wifi + failure_rate_wwan + failure_rate_wimax + failure_rate_ethernet) AS failure_rate, c_date\nFROM {table_name}\n{where_filter}\nGROUP BY c_date', '2');
INSERT INTO `report_queries` VALUES ('38', '1', '50', '2011-06-27 23:48:31', '2011-06-27 23:48:31', 'line', '{\n\"color\": \"red\"\n}', '', 'failure_rate', '', '', '', 'SELECT day_of_weeks.name, (h.failure_rate_wifi + h.failure_rate_wwan + h.failure_rate_wimax + h.failure_rate_ethernet) AS failure_rate\nFROM day_of_weeks\nLEFT JOIN {table_name} AS h\nON day_of_weeks.name = h.day_of_week\nGROUP BY day_of_weeks.name\nORDER BY day_of_weeks.id', '2');
INSERT INTO `report_queries` VALUES ('39', '17', '66', '2011-07-06 21:20:13', '2011-07-06 21:20:13', null, null, null, null, null, null, '', 'SELECT heat_lat, heat_lng, heat_x, heat_y, heat_num, connections\nFROM {table_name}\nGROUP BY heat_lat, heat_lng, heat_x, heat_y, heat_num\n{and_filter}', '0');
INSERT INTO `report_queries` VALUES ('40', '16', '63', '2011-07-06 21:40:26', '2011-09-01 00:45:08', null, null, null, null, null, null, '', 'SELECT SUM(throughput_per_day) AS throughput_per_day, AVG(avg_signal_strength_percent) AS avg_signal_strength_percent, server_date\r\nFROM {table_name}\r\nGROUP BY server_date\r\nORDER BY server_date ASC', '0');
INSERT INTO `report_queries` VALUES ('42', '6', '12', '2011-07-06 22:17:41', '2011-07-06 22:17:41', 'bar', '', '', 'foo', '', '', '', '', '0');
INSERT INTO `report_queries` VALUES ('43', '18', '67', '2011-07-06 22:31:02', '2011-07-06 22:31:02', null, null, null, null, null, null, '', 'SELECT heat_lat, heat_lng, heat_x, heat_y, heat_num, throughput\nFROM {table_name}\nGROUP BY heat_lat, heat_lng, heat_x, heat_y, heat_num\n{and_filter}', '0');
INSERT INTO `report_queries` VALUES ('44', '19', '68', '2011-07-06 22:45:54', '2011-07-06 22:45:54', null, null, null, null, null, null, '', 'SELECT heat_lat, heat_lng, heat_x, heat_y, heat_num, throughput\nFROM {table_name}\nGROUP BY heat_lat, heat_lng, heat_x, heat_y, heat_num\n{and_filter}', '0');
INSERT INTO `report_queries` VALUES ('45', '20', '69', '2011-07-06 22:54:18', '2011-07-06 22:54:18', null, null, null, null, null, null, '', 'SELECT heat_lat, heat_lng, heat_x, heat_y, heat_num, signal_strength\nFROM {table_name}\nGROUP BY heat_lat, heat_lng, heat_x, heat_y, heat_num\n{and_filter}', '0');
INSERT INTO `report_queries` VALUES ('46', '21', '70', '2011-07-06 22:58:39', '2011-07-06 22:58:39', null, null, null, null, null, null, '', 'SELECT heat_lat, heat_lng, heat_x, heat_y, heat_num, peak_speed\nFROM {table_name}\nGROUP BY heat_lat, heat_lng, heat_x, heat_y, heat_num\n{and_filter}', '0');
INSERT INTO `report_queries` VALUES ('47', '22', null, '2011-07-06 23:03:27', '2011-08-31 01:48:18', null, null, null, null, null, null, '', 'SELECT heat_lat, heat_lng, heat_x, heat_y, heat_num, error_freq\r\nFROM {table_name}\r\nGROUP BY heat_lat, heat_lng, heat_x, heat_y, heat_num\r\n{and_filter}', '0');
INSERT INTO `report_queries` VALUES ('48', '23', '72', '2011-07-06 23:07:20', '2011-07-06 23:07:20', null, null, null, null, null, null, '', 'SELECT heat_lat, heat_lng, heat_x, heat_y, heat_num, num_failures\nFROM {table_name}\nGROUP BY heat_lat, heat_lng, heat_x, heat_y, heat_num\n{and_filter}', '0');
INSERT INTO `report_queries` VALUES ('49', '24', '44', '2011-08-31 01:53:36', '2011-08-31 05:32:13', null, null, null, null, null, null, '', 'SELECT SUM(cnt_current_total) AS cnt_current_total, server_date FROM {table_name} GROUP BY server_date ORDER BY server_date ASC;', '0');
INSERT INTO `reports` VALUES ('1', 'Weekly Usage Trend', '2011-05-27 00:51:12', '2011-06-27 23:48:31', '{\n    \"chart\": {\n        \"zoomType\": \"xy\"\n    },\n    \"xAxis\": {\n        \"categories\": [\n            \"Monday\",\n            \"Tuesday\",\n            \"Wednesday\",\n            \"Thursday\",\n            \"Friday\",\n            \"Saturday\",\n            \"Sunday\"\n        ]\n    },\n    \"yAxis\": [\n        {\n            \"title\": {\n                \"text\": \"Connections / Day\",\n                \"style\": {\n                    \"color\": \"#21415E\"\n                }\n            }\n        },\n        {\n            \"title\": {\n                \"text\": \"Average Data Uploaded and Downloaded\",\n                \"style\": {\n                    \"color\": \"#8DAF17\"\n                }\n            },\n            \"opposite\": true\n        },\n        {\n            \"title\": {\n                \"text\": \"Failure Rate (%)\",\n                \"style\": {\n                    \"color\": \"red\"\n                }\n            },\n            \"opposite\": true\n        }\n    ],\n    \"plotOptions\": {\n        \"area\": {\n            \"stacking\": \"normal\",\n            \"lineColor\": \"#666666\",\n            \"lineWidth\": 1,\n            \"marker\": {\n                \"lineWidth\": 1,\n                \"lineColor\": \"#666666\"\n            }\n        }\n    }\n}', 'ChartReport', null, '0', '1');
INSERT INTO `reports` VALUES ('2', 'Error Scatterplot', '2011-05-27 00:51:12', '2011-06-28 21:54:38', '{\n    \"chart\": {\n        \"zoomType\": \"xy\"\n    },\n    \"xAxis\": {\n        \"text\": \"Frequency/hr\",\n        \"min\": 0\n    },\n    \"yAxis\": {\n        \"title\": {\n            \"text\": \"% Users Affected\",\n            \"min\": 0,\n            \"max\": 100,\n            \"tickInterval\": 10\n        }\n    },\n    \"plotOptions\": {\n        \"scatter\": {\n            \"marker\": {\n                \"radius\": \"5\",\n                \"states\": {\n                    \"hover\": {\n                        \"enabled\": \"true\",\n                        \"lineColor\": \"rgb(250,250,100)\"\n                    }\n                }\n            },\n            \"states\": {\n                \"hover\": {\n                    \"marker\": {\n                        \"enabled\": \"false\"\n                    }\n                }\n            }\n        }\n    }\n}', 'ChartReport', null, '0', '1');
INSERT INTO `reports` VALUES ('3', 'Usage Trend', '2011-05-27 00:51:12', '2011-07-21 00:38:35', '{\n    \"chart\": {\n        \"zoomType\": \"xy\"\n    },\n    \"xAxis\": {\n        \"type\": \"datetime\",\n        \"tickInterval\": 2592000000\n    },\n    \"yAxis\": [\n        {\n            \"title\": {\n                \"text\": \"Connections / Day\",\n                \"style\": {\n                    \"color\": \"#21415E\"\n                }\n            }\n        },\n        {\n            \"title\": {\n                \"text\": \"Average Data Uploaded and Downloaded\",\n                \"style\": {\n                    \"color\": \"#8DAF17\"\n                }\n            },\n            \"opposite\": true\n        },\n        {\n            \"title\": {\n                \"text\": \"Failure Rate (%)\",\n                \"style\": {\n                    \"color\": \"red\"\n                }\n            },\n            \"opposite\": true\n        }\n    ],\n    \"plotOptions\": {\n        \"area\": {\n            \"stacking\": \"normal\",\n            \"lineColor\": \"#666666\",\n            \"lineWidth\": 1,\n            \"marker\": {\n                \"lineWidth\": 1,\n                \"lineColor\": \"#666666\"\n            }\n        }\n    }\n}', 'ChartReport', null, '0', '1');
INSERT INTO `reports` VALUES ('4', 'Data by Technology', '2011-05-27 00:51:12', '2011-07-21 00:32:47', '{\n    \"chart\": {\n        \"defaultSeriesType\": \"area\"\n    },\n    \"xAxis\": {\n        \"type\": \"datetime\",\n        \"tickInterval\": 2592000000\n    },\n    \"yAxis\": {\n        \"title\": {\n            \"text\": \"Average Data Uploaded and Downloaded\"\n        }\n    },\n    \"plotOptions\": {\n        \"area\": {\n            \"stacking\": \"normal\",\n            \"lineColor\": \"#666666\",\n            \"lineWidth\": 1,\n            \"marker\": {\n                \"lineWidth\": 1,\n                \"lineColor\": \"#666666\"\n            }\n        }\n    }\n}', 'ChartReport', null, '0', '1');
INSERT INTO `reports` VALUES ('5', 'Daily Usage Trend', '2011-05-27 00:51:12', '2011-06-28 16:45:45', '{\n    \"chart\": {\n        \"zoomType\": \"xy\"\n    },\n    \"xAxis\": {\n        \"type\": \"datetime\",\n        \"tickInterval\": 10800000,\n        \"dateTimeLabelFormats\": {\n            \"day\": \"%H:%M\"\n        }\n    },\n    \"yAxis\": [\n        {\n            \"title\": {\n                \"text\": \"Connections / Hour\",\n                \"style\": {\n                    \"color\": \"#21415E\"\n                }\n            }\n        },\n        {\n            \"title\": {\n                \"text\": \"Average Data Uploaded and Downloaded\",\n                \"style\": {\n                    \"color\": \"#8DAF17\"\n                }\n            },\n            \"opposite\": true\n        },\n        {\n            \"title\": {\n                \"text\": \"Failure Rate (%)\",\n                \"style\": {\n                    \"color\": \"red\"\n                }\n            },\n            \"opposite\": true\n        }\n    ],\n    \"plotOptions\": {\n        \"line\": {\n            \"zIndex\": 1\n        },\n        \"area\": {\n            \"zIndex\": 0,\n            \"stacking\": \"normal\",\n            \"lineColor\": \"#666666\",\n            \"lineWidth\": 1,\n            \"marker\": {\n                \"lineWidth\": 1,\n                \"lineColor\": \"#666666\"\n            }\n        }\n    }\n}', 'ChartReport', null, '0', '1');
INSERT INTO `reports` VALUES ('6', 'Licensing Report', '2011-05-27 00:51:12', '2011-07-06 22:17:41', '{\n    \"chart\": {\n        \"zoomType\": \"xy\"\n    },\n    \"xAxis\": {\n        \"type\": \"datetime\",\n        \"tickInterval\": 2592000000\n    },\n    \"yAxis\": {\n        \"title\": {\n            \"text\": \"Total Number of Customers\"\n        }\n    },\n    \"credits\": {\n        \"enabled\": false\n    },\n    \"plotOptions\": {\n        \"column\": {\n            \"stacking\": \"normal\"\n        }\n    }\n}', 'ChartReport', null, '4', '1');
INSERT INTO `reports` VALUES ('7', 'Device Usage Pie Chart', '2011-05-27 00:51:12', '2011-05-29 00:57:45', '{\"chart\":{\"plotShadow\":\"false\"},\"title\":{\"text\":\"Device usage over past week\"},\"plotOptions\":{\"allowPointSelect\":\"true\",\"cursor\":\"pointer\"}}', 'ChartReport', null, '0', '1');
INSERT INTO `reports` VALUES ('8', 'Error Report', '2011-05-27 00:51:12', '2011-05-29 06:25:57', null, 'TableReport', 'error_code, error_msg, connection_type, percent_user_affected, frequency_per_hour, total_cnt_error', '0', '1');
INSERT INTO `reports` VALUES ('9', 'Technology Performance Report', '2011-05-28 07:01:42', '2011-05-28 07:55:05', null, 'TableReport', 'connection_type, conn_per_day, failed_conn_per_day, dropped_per_day, avg_time_conn_per_day, avg_time_conn_per_session, avg_downloaded_per_day, avg_uploaded_per_day, avg_download_speed, avg_upload_speed, avg_signal_strength', '0', '1');
INSERT INTO `reports` VALUES ('10', 'Technology Performance Summary', '2011-05-28 21:19:00', '2011-05-28 23:23:19', null, 'TableReport', 'connection_type, conn_per_day, throughput_per_day, avg_download_speed, avg_signal_strength', '0', '1');
INSERT INTO `reports` VALUES ('11', 'Device Usage Report', '2011-05-29 02:55:52', '2011-07-21 00:37:13', null, 'TableReport', 'manufacturer, device_model, user_share, sum_users, avg_conn_time_per_day,  avg_data_upload_per_day,  avg_data_download_per_day, avg_peak_download_speed, avg_peak_upload_speed', '0', '1');
INSERT INTO `reports` VALUES ('13', 'Device Usage Summary', '2011-05-29 03:06:48', '2011-07-21 00:37:45', null, 'TableReport', 'manufacturer, device_model, user_share, avg_time_conn_per_day, avg_throughput_per_day', '1', '1');
INSERT INTO `reports` VALUES ('14', 'Error Summary', '2011-05-29 06:40:57', '2011-05-29 06:40:57', null, 'TableReport', 'error_code, error_msg, percent_user_affected, frequency_per_hour', '0', '1');
INSERT INTO `reports` VALUES ('15', 'Total Connections Heat Map', '2011-06-07 20:36:54', '2011-07-06 21:00:16', null, 'HeatMapReport', 'heat_lat, heat_lng, heat_x, heat_y, heat_num, sum_conn_attempts', '3', '1');
INSERT INTO `reports` VALUES ('16', 'KPI Report', '2011-06-13 23:50:13', '2011-06-14 00:56:50', null, 'KpiReport', 'kpi, actual, target', '0', '1');
INSERT INTO `reports` VALUES ('17', 'Connections per Day Heat Map', '2011-07-06 21:20:13', '2011-07-06 21:20:13', null, 'HeatMapReport', 'heat_lat, heat_lng, heat_x, heat_y, heat_num, connections', '3', '1');
INSERT INTO `reports` VALUES ('18', 'Total Throughput Heat Map ', '2011-07-06 22:31:02', '2011-07-06 22:40:52', null, 'HeatMapReport', 'heat_lat, heat_lng, heat_x, heat_y, heat_num, throughput', '3', '1');
INSERT INTO `reports` VALUES ('19', 'Throughput per Day Heat Map', '2011-07-06 22:45:54', '2011-07-06 22:49:24', null, 'HeatMapReport', 'heat_lat, heat_lng, heat_x, heat_y, heat_num, throughput', '3', '1');
INSERT INTO `reports` VALUES ('20', 'Average Signal Strength Heat Map', '2011-07-06 22:54:18', '2011-07-06 22:54:18', null, 'HeatMapReport', 'heat_lat, heat_lng, heat_x, heat_y, heat_num, signal_strength', '3', '1');
INSERT INTO `reports` VALUES ('21', 'Average Peak Download Speed Heat Map', '2011-07-06 22:58:38', '2011-07-06 22:58:38', null, 'HeatMapReport', 'heat_lat, heat_lng, heat_x, heat_y, heat_num, peak_speed', '3', '1');
INSERT INTO `reports` VALUES ('22', 'Error Frequency Heat Map', '2011-07-06 23:03:27', '2011-07-06 23:03:27', null, 'HeatMapReport', 'heat_lat, heat_lng, heat_x, heat_y, heat_num, error_freq', '3', '1');
INSERT INTO `reports` VALUES ('23', 'Connection Failure Rate Heat Map', '2011-07-06 23:07:20', '2011-07-06 23:07:20', null, 'HeatMapReport', 'heat_lat, heat_lng, heat_x, heat_y, heat_num, num_failures', '3', '1');
INSERT INTO `reports` VALUES ('24', 'Data Target', '2011-08-31 01:37:33', '2011-08-31 23:45:31', null, 'KpiReport', 'kpi, actual, target', '0', null);
INSERT INTO `reports` VALUES ('25', 'Daves KPIs', '2011-09-07 18:09:24', '2011-09-07 18:09:24', null, 'KpiReport', '', '0', null);
INSERT INTO `roles` VALUES ('1', 'administrator', '2011-05-27 00:51:11', '2011-05-27 00:51:11', null);
INSERT INTO `roles` VALUES ('2', 'portal user', '2011-05-27 00:51:42', '2011-05-27 00:51:42', null);
INSERT INTO `roles_users` VALUES ('1', '2');
INSERT INTO `roles_users` VALUES ('1', '9');
INSERT INTO `roles_users` VALUES ('2', '3');
INSERT INTO `roles_users` VALUES ('2', '4');
INSERT INTO `roles_users` VALUES ('2', '5');
INSERT INTO `roles_users` VALUES ('2', '6');
INSERT INTO `roles_users` VALUES ('2', '7');
INSERT INTO `roles_users` VALUES ('2', '8');
INSERT INTO `schema_migrations` VALUES ('20101216231920');
INSERT INTO `schema_migrations` VALUES ('20101220171206');
INSERT INTO `schema_migrations` VALUES ('20101221173657');
INSERT INTO `schema_migrations` VALUES ('20101221180641');
INSERT INTO `schema_migrations` VALUES ('20101228202057');
INSERT INTO `schema_migrations` VALUES ('20101228210908');
INSERT INTO `schema_migrations` VALUES ('20101229231001');
INSERT INTO `schema_migrations` VALUES ('20101229232555');
INSERT INTO `schema_migrations` VALUES ('20110205000352');
INSERT INTO `schema_migrations` VALUES ('20110314203329');
INSERT INTO `schema_migrations` VALUES ('20110317123314');
INSERT INTO `schema_migrations` VALUES ('20110317123612');
INSERT INTO `schema_migrations` VALUES ('20110317132925');
INSERT INTO `schema_migrations` VALUES ('20110322214815');
INSERT INTO `schema_migrations` VALUES ('20110323222629');
INSERT INTO `schema_migrations` VALUES ('20110331075732');
INSERT INTO `schema_migrations` VALUES ('20110401231719');
INSERT INTO `schema_migrations` VALUES ('20110401231958');
INSERT INTO `schema_migrations` VALUES ('20110401232032');
INSERT INTO `schema_migrations` VALUES ('20110406193744');
INSERT INTO `schema_migrations` VALUES ('20110407065721');
INSERT INTO `schema_migrations` VALUES ('20110407164559');
INSERT INTO `schema_migrations` VALUES ('20110407175112');
INSERT INTO `schema_migrations` VALUES ('20110410044205');
INSERT INTO `schema_migrations` VALUES ('20110412235451');
INSERT INTO `schema_migrations` VALUES ('20110414204113');
INSERT INTO `schema_migrations` VALUES ('20110419201116');
INSERT INTO `schema_migrations` VALUES ('20110422015231');
INSERT INTO `schema_migrations` VALUES ('20110428073749');
INSERT INTO `schema_migrations` VALUES ('20110428091731');
INSERT INTO `schema_migrations` VALUES ('20110428212737');
INSERT INTO `schema_migrations` VALUES ('20110428212805');
INSERT INTO `schema_migrations` VALUES ('20110501071727');
INSERT INTO `schema_migrations` VALUES ('20110505221146');
INSERT INTO `schema_migrations` VALUES ('20110506233257');
INSERT INTO `schema_migrations` VALUES ('20110508225542');
INSERT INTO `schema_migrations` VALUES ('20110510224230');
INSERT INTO `schema_migrations` VALUES ('20110518075811');
INSERT INTO `schema_migrations` VALUES ('20110518102252');
INSERT INTO `schema_migrations` VALUES ('20110519094120');
INSERT INTO `schema_migrations` VALUES ('20110522232908');
INSERT INTO `schema_migrations` VALUES ('20110528020138');
INSERT INTO `schema_migrations` VALUES ('20110606062349');
INSERT INTO `schema_migrations` VALUES ('20110606124147');
INSERT INTO `schema_migrations` VALUES ('20110607051842');
INSERT INTO `schema_migrations` VALUES ('20110610000402');
INSERT INTO `schema_migrations` VALUES ('20110614101412');
INSERT INTO `schema_migrations` VALUES ('20110614101438');
INSERT INTO `schema_migrations` VALUES ('20110617224908');
INSERT INTO `schema_migrations` VALUES ('20110619075408');
INSERT INTO `schema_migrations` VALUES ('20110706232909');
INSERT INTO `schema_migrations` VALUES ('20110711214225');
INSERT INTO `schema_migrations` VALUES ('20110715004954');
INSERT INTO `taggings` VALUES ('1', '1', '1', 'Report', null, null, 'tags', '2011-05-27 00:51:12');
INSERT INTO `taggings` VALUES ('2', '2', '2', 'Report', null, null, 'tags', '2011-05-27 00:51:12');
INSERT INTO `taggings` VALUES ('3', '1', '3', 'Report', null, null, 'tags', '2011-05-27 00:51:12');
INSERT INTO `taggings` VALUES ('4', '1', '4', 'Report', null, null, 'tags', '2011-05-27 00:51:12');
INSERT INTO `taggings` VALUES ('5', '1', '5', 'Report', null, null, 'tags', '2011-05-27 00:51:12');
INSERT INTO `taggings` VALUES ('8', '2', '8', 'Report', null, null, 'tags', '2011-05-27 00:51:12');
INSERT INTO `taggings` VALUES ('9', '3', '9', 'Report', null, null, 'tags', '2011-05-28 07:01:43');
INSERT INTO `taggings` VALUES ('10', '3', '10', 'Report', null, null, 'tags', '2011-05-28 21:19:00');
INSERT INTO `taggings` VALUES ('13', '1', '13', 'Report', null, null, 'tags', '2011-05-29 03:06:48');
INSERT INTO `taggings` VALUES ('14', '2', '14', 'Report', null, null, 'tags', '2011-05-29 06:40:57');
INSERT INTO `taggings` VALUES ('16', '3', '15', 'Report', null, null, 'tags', '2011-06-07 21:13:33');
INSERT INTO `taggings` VALUES ('17', '5', '16', 'Report', null, null, 'tags', '2011-06-13 23:50:14');
INSERT INTO `taggings` VALUES ('20', '1', '7', 'Report', null, null, 'tags', '2011-06-30 20:18:01');
INSERT INTO `taggings` VALUES ('22', '1', '11', 'Report', null, null, 'tags', '2011-07-05 22:05:18');
INSERT INTO `taggings` VALUES ('24', '3', '18', 'Report', null, null, 'tags', '2011-07-06 22:31:03');
INSERT INTO `taggings` VALUES ('26', '3', '20', 'Report', null, null, 'tags', '2011-07-06 22:54:18');
INSERT INTO `taggings` VALUES ('27', '3', '21', 'Report', null, null, 'tags', '2011-07-06 22:58:39');
INSERT INTO `taggings` VALUES ('29', '3', '23', 'Report', null, null, 'tags', '2011-07-06 23:07:20');
INSERT INTO `taggings` VALUES ('30', '2', '22', 'Report', null, null, 'tags', '2011-07-07 03:32:36');
INSERT INTO `taggings` VALUES ('31', '1', '17', 'Report', null, null, 'tags', '2011-07-07 03:33:09');
INSERT INTO `taggings` VALUES ('32', '1', '19', 'Report', null, null, 'tags', '2011-07-07 03:33:15');
INSERT INTO `taggings` VALUES ('33', '5', '24', 'Report', null, null, 'tags', '2011-08-31 01:37:33');
INSERT INTO `tags` VALUES ('1', 'Customer Profiling');
INSERT INTO `tags` VALUES ('2', 'Error Management');
INSERT INTO `tags` VALUES ('3', 'Network Performance');
INSERT INTO `tags` VALUES ('4', 'Heat Map');
INSERT INTO `tags` VALUES ('5', 'My Reports');
INSERT INTO `user_dashboards` VALUES ('1', '2', '1', null, '2011-05-29 07:46:31', '2011-05-29 07:46:31');
INSERT INTO `user_dashboards` VALUES ('2', '1', '2', null, '2011-08-31 01:30:04', '2011-08-31 01:30:04');
INSERT INTO `user_dashboards` VALUES ('3', '6', '3', null, '2011-08-31 01:43:08', '2011-08-31 01:43:08');
INSERT INTO `user_dashboards` VALUES ('4', '9', '4', null, '2011-08-31 01:43:57', '2011-08-31 01:43:57');
INSERT INTO `user_dashboards` VALUES ('5', '8', '5', null, '2011-08-31 23:57:33', '2011-08-31 23:57:33');
INSERT INTO `user_dashboards` VALUES ('6', '9', '6', null, '2011-09-02 12:13:34', '2011-09-02 12:13:34');
INSERT INTO `user_dashboards` VALUES ('7', '2', '7', null, '2011-09-07 17:10:19', '2011-09-07 17:10:19');
INSERT INTO `user_dashboards` VALUES ('8', '2', '8', null, '2011-09-07 17:12:02', '2011-09-07 17:12:02');
INSERT INTO `users` VALUES ('1', 'root', 'user', 'root@analytics.local', '$2a$10$bUUIacEpXSAOaN.ruF9MzOSjvESXYv7T8aZTeRQFd.wRgCJmVoojW', null, null, null, '127', '2011-09-07 18:11:34', '2011-09-07 17:42:52', '10.0.1.25', '10.0.1.25', '2011-05-27 00:51:11', '2011-09-07 18:11:34', null, null, null, null, null, '1');
INSERT INTO `users` VALUES ('2', 'admin', 'user', 'admin@analytics.local', '$2a$10$moroVz0iEjfofkAqFdO3HO69Lg0bxxhSBML2HWzLJjqJrsftdvDgu', null, null, null, '61', '2011-09-07 18:10:45', '2011-09-07 17:02:01', '10.0.1.25', '172.30.22.115', '2011-05-27 00:51:11', '2011-09-07 18:10:45', null, null, null, null, null, '0');
INSERT INTO `users` VALUES ('3', 'portal', 'user', 'portal@analytics.local', '$2a$10$ojgYWtjjENGKl3FJlHqAc.IBQ.0eYME4U6iA7xVDpDF9LEJ.CmjXm', null, null, null, '5', '2011-09-06 18:14:50', '2011-09-05 18:44:45', '10.110.129.112', '10.110.129.112', '2011-05-27 00:51:43', '2011-09-06 18:14:50', null, null, null, null, null, '0');
INSERT INTO `users` VALUES ('4', 'Sergei', 'Prutkin', 'sprutkin@smithmicro.com', '$2a$10$C7iVqJRKzuhdP8JF9eR8w.ARj7mtvUjOOznZC9DAdjNnj3qYTOxBO', null, null, null, '0', null, null, null, null, '2011-05-27 00:51:43', '2011-05-27 00:51:43', null, null, null, null, null, '0');
INSERT INTO `users` VALUES ('5', 'Tonya', 'Hernandez', 'thernandez2@smithmicro.com', '$2a$10$SwvcXgDUVBE/1uueAGxDp.DQ.BhFKgRQ1ZwnZU5fKAiih.pcZh5My', null, null, null, '3', '2011-06-28 19:15:00', '2011-06-28 17:22:45', '10.0.1.36', '10.0.1.36', '2011-05-27 00:51:43', '2011-06-28 19:15:00', null, null, null, null, null, '0');
INSERT INTO `users` VALUES ('6', 'David', 'Terry', 'dterry@smithmicro.com', '$2a$10$wSk2Kv8nAzILICIzqDqpvuwvQ.tN.a2t5nSZGD6C6cvgy2kv5OBpW', null, null, null, '15', '2011-08-31 01:42:57', '2011-07-07 18:32:50', '10.0.1.32', '10.0.1.146', '2011-05-27 00:51:43', '2011-08-31 01:42:57', null, null, null, null, null, '0');
INSERT INTO `users` VALUES ('7', 'Tele', 'Com', 'telecom1@smithmicro.com', '$2a$10$3m1jMLmf3lrXmAaMjNyb2OjQCa6ttzJMnXLjhW51mFs7FYDfpnKkm', null, null, null, '42', '2011-07-07 22:58:59', '2011-07-06 21:52:36', '172.30.22.134', '223.13.13.67', '2011-06-28 16:18:44', '2011-07-07 23:01:21', null, '2011-06-28 16:18:44', null, null, null, '0');
INSERT INTO `users` VALUES ('8', 'Rally', 'User', 'rally@smithmicro.com', '$2a$10$n7Dvz6DNmmQWuen9UEUHreSGUz89qoLib2IqzlyKsDuJvenMeIMfi', null, null, null, '21', '2011-09-01 16:11:43', '2011-08-31 23:57:14', '86.2.106.208', '10.0.1.32', '2011-07-06 21:17:36', '2011-09-01 16:11:43', null, '2011-07-06 21:17:36', null, null, null, '0');
INSERT INTO `users` VALUES ('9', 'Rally', 'Admin', 'rallyadmin@smithmicro.com', '$2a$10$q4G5fy6Yf./X6xs1l1aPu.RRYXmOPn1/pB1O6uTw1/QHAf63dxW3.', null, null, null, '27', '2011-09-02 12:08:41', '2011-09-01 16:16:17', '31.64.61.129', '86.2.106.208', '2011-07-06 22:04:15', '2011-09-02 12:08:41', null, '2011-07-06 22:04:15', null, null, null, '0');
