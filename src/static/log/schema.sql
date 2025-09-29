CREATE TABLE IF NOT EXISTS `pipeline_logs` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `pipe_name` varchar(255) NOT NULL,
  `manual_trigger` tinyint(1) NOT NULL DEFAULT '0',
  `status_id` int NOT NULL,
  `duration` int NOT NULL,
  `extracted_rows` int NOT NULL,
  `dataframes_memory` int NOT NULL,
  `loaded_rows` int NOT NULL,
  `uuid` binary(16) NOT NULL,
  `error` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `uuid` (`uuid`) USING BTREE,
  KEY `pipe_name` (`pipe_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;