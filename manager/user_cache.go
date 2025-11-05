package manager

import (
	"crypto-mine/orm"
	"crypto-mine/orm/models"
	"sync"
	"time"

	"log/slog"
)

// UserWithPreferences holds notification preferences data including email and telegram_id
type UserWithPreferences struct {
	Preferences *models.NotificationPreferences
}

// UserCacheManager manages notification preferences in memory
type UserCacheManager struct {
	ORMClient         *orm.Client
	db                *orm.DB
	userData          map[int]*UserWithPreferences // userID -> UserWithPreferences
	dataMutex         sync.RWMutex
	refreshInterval   time.Duration
	syncInterval      time.Duration // Interval for record_refresh sync cron job
	refreshLimit      int
	syncLimit         int // Maximum number of records to process per sync cycle
	lastFetchedPrefID int
	lastFetchedMutex  sync.RWMutex
	stopChan          chan struct{}
	logger            *slog.Logger
}

// NewUserCacheManager creates a new user cache manager
func NewUserCacheManager(ormClient *orm.Client, refreshInterval time.Duration) *UserCacheManager {
	return &UserCacheManager{
		ORMClient:         ormClient,
		db:                orm.NewDB(ormClient),
		userData:          make(map[int]*UserWithPreferences),
		refreshInterval:   refreshInterval,
		syncInterval:      30 * time.Second, // Default: check every 30 seconds
		refreshLimit:      100,              // Default: fetch 100 records per refresh
		syncLimit:         50,               // Default: process 50 records per sync cycle
		lastFetchedPrefID: 0,
		stopChan:          make(chan struct{}),
		logger:            slog.Default(),
	}
}

// SetRefreshLimit sets the maximum number of records to fetch per refresh
func (m *UserCacheManager) SetRefreshLimit(limit int) {
	if limit > 0 {
		m.refreshLimit = limit
	}
}

// SetLogger sets a custom logger
func (m *UserCacheManager) SetLogger(logger *slog.Logger) {
	m.logger = logger
}

// Start starts the refresh goroutines
func (m *UserCacheManager) Start() error {
	m.logger.Info("Starting user cache manager")

	// Initial refresh
	if err := m.Refresh(); err != nil {
		m.logger.Error("Failed to refresh user cache on startup", "error", err)
		return err
	}

	// Start periodic refresh
	go m.refreshLoop()

	// Start record_refresh sync cron job
	go m.syncLoop()

	return nil
}

// Stop stops the refresh goroutines
func (m *UserCacheManager) Stop() {
	m.logger.Info("Stopping user cache manager")
	close(m.stopChan)
}

// Refresh fetches notification preferences from the database incrementally
func (m *UserCacheManager) Refresh() error {
	m.logger.Debug("Refreshing user cache from database")

	// Refresh notification preferences
	if err := m.refreshNotificationPreferences(); err != nil {
		m.logger.Error("Failed to refresh notification preferences", "error", err)
		return err
	}

	return nil
}

// refreshNotificationPreferences fetches notification preferences incrementally
func (m *UserCacheManager) refreshNotificationPreferences() error {
	m.lastFetchedMutex.RLock()
	lastFetchedID := m.lastFetchedPrefID
	m.lastFetchedMutex.RUnlock()

	// If no last fetched ID, get the maximum ID from memory
	if lastFetchedID == 0 {
		m.dataMutex.RLock()
		maxID := m.getMaxPrefIDInMemory()
		m.dataMutex.RUnlock()
		lastFetchedID = maxID
	}

	// Build SQL query with incremental fetching
	// Exclude record_refresh = true records (they're synced separately)
	sql := `
		SELECT id, user_id, email, telegram_id, email_enabled, telegram_enabled, record_refresh, created_at, updated_at
		FROM notification_preferences
		WHERE id > ? AND (record_refresh = 0 OR record_refresh IS NULL)
		ORDER BY id ASC
		LIMIT ?
	`

	results, err := m.ORMClient.ExecuteSQL(sql, lastFetchedID, m.refreshLimit)
	if err != nil {
		return err
	}

	if len(results) == 0 {
		return nil
	}

	// Convert results to NotificationPreferences models and update cache
	var maxID int
	m.dataMutex.Lock()
	for _, row := range results {
		prefs := &models.NotificationPreferences{}
		if err := orm.ConvertRowToModel(row, prefs); err != nil {
			m.logger.Warn("Failed to convert row to NotificationPreferences", "error", err)
			continue
		}
		// Apply defaults if not set
		if _, ok := row["email_enabled"]; !ok {
			prefs.EmailEnabled = false
		}
		if _, ok := row["telegram_enabled"]; !ok {
			prefs.TelegramEnabled = false
		}

		// Get or create UserWithPreferences entry
		if m.userData[prefs.UserID] == nil {
			m.userData[prefs.UserID] = &UserWithPreferences{
				Preferences: prefs,
			}
		} else {
			// Update existing preferences
			m.userData[prefs.UserID].Preferences = prefs
		}

		if prefs.ID > maxID {
			maxID = prefs.ID
		}
	}
	m.dataMutex.Unlock()

	// Update last fetched ID
	if maxID > 0 {
		m.lastFetchedMutex.Lock()
		m.lastFetchedPrefID = maxID
		m.lastFetchedMutex.Unlock()
	}

	return nil
}

// getMaxPrefIDInMemory returns the maximum preference ID from memory
func (m *UserCacheManager) getMaxPrefIDInMemory() int {
	maxID := 0
	for _, data := range m.userData {
		if data.Preferences != nil && data.Preferences.ID > maxID {
			maxID = data.Preferences.ID
		}
	}
	return maxID
}

// refreshLoop periodically refreshes the user cache
func (m *UserCacheManager) refreshLoop() {
	ticker := time.NewTicker(m.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.Refresh(); err != nil {
				m.logger.Error("Failed to refresh user cache", "error", err)
			}
		case <-m.stopChan:
			return
		}
	}
}

// SyncRecordRefreshPreferences syncs notification preferences with record_refresh = true
// This cron job finds records that need to be synced (record_refresh = true),
// updates them in memory, then sets record_refresh = false
func (m *UserCacheManager) SyncRecordRefreshPreferences() error {
	m.logger.Debug("Syncing notification preferences with record_refresh flag")

	// Query for preferences that need to be synced (record_refresh = true)
	sql := `
		SELECT id, user_id, email, telegram_id, email_enabled, telegram_enabled, record_refresh, created_at, updated_at
		FROM notification_preferences
		WHERE record_refresh = 1
		LIMIT ?
	`

	results, err := m.ORMClient.ExecuteSQL(sql, m.syncLimit)
	if err != nil {
		m.logger.Error("Failed to fetch notification preferences with record_refresh flag", "error", err)
		return err
	}

	if len(results) == 0 {
		m.logger.Debug("No notification preferences found with record_refresh flag")
		return nil
	}

	// Convert results to NotificationPreferences models
	preferences := make([]models.NotificationPreferences, 0, len(results))
	idsToUpdate := make([]int, 0, len(results))
	for _, row := range results {
		prefs := models.NotificationPreferences{}
		if err := orm.ConvertRowToModel(row, &prefs); err != nil {
			m.logger.Warn("Failed to convert row to NotificationPreferences", "error", err)
			continue
		}
		// Apply defaults if not set
		if _, ok := row["email_enabled"]; !ok {
			prefs.EmailEnabled = false
		}
		if _, ok := row["telegram_enabled"]; !ok {
			prefs.TelegramEnabled = false
		}
		preferences = append(preferences, prefs)
		idsToUpdate = append(idsToUpdate, prefs.ID)
	}

	m.logger.Debug("Found notification preferences to sync", "count", len(preferences))

	// Sync with memory: update preferences in cache
	m.dataMutex.Lock()
	updatedCount := 0
	for i := range preferences {
		prefs := &preferences[i]

		// Get or create UserWithPreferences entry
		if m.userData[prefs.UserID] == nil {
			m.userData[prefs.UserID] = &UserWithPreferences{
				Preferences: prefs,
			}
			updatedCount++
		} else {
			// Update existing preferences
			m.userData[prefs.UserID].Preferences = prefs
			updatedCount++
		}
	}
	m.dataMutex.Unlock()

	// Update record_refresh = false for all processed records
	if len(idsToUpdate) > 0 {
		// Build batch update query
		updateSQL := `UPDATE notification_preferences SET record_refresh = 0 WHERE id IN (`
		params := make([]interface{}, len(idsToUpdate))
		for i, id := range idsToUpdate {
			if i > 0 {
				updateSQL += ","
			}
			updateSQL += "?"
			params[i] = id
		}
		updateSQL += ")"

		_, err := m.ORMClient.ExecuteSQLUpdate(updateSQL, params...)
		if err != nil {
			m.logger.Warn("Failed to update record_refresh flag", "error", err)
		}
	}

	m.logger.Info("Synced notification preferences with record_refresh flag",
		"total_count", len(preferences),
		"updated_count", updatedCount)

	return nil
}

// syncLoop periodically syncs notification preferences with record_refresh flag
func (m *UserCacheManager) syncLoop() {
	ticker := time.NewTicker(m.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.SyncRecordRefreshPreferences(); err != nil {
				m.logger.Error("Failed to sync record_refresh preferences", "error", err)
			}
		case <-m.stopChan:
			return
		}
	}
}

// GetNotificationPreferences returns notification preferences from cache by user ID
func (m *UserCacheManager) GetNotificationPreferences(userID int) *models.NotificationPreferences {
	m.dataMutex.RLock()
	defer m.dataMutex.RUnlock()

	if data, exists := m.userData[userID]; exists && data.Preferences != nil {
		// Return a copy to avoid race conditions
		prefsCopy := *data.Preferences
		return &prefsCopy
	}

	// Return defaults if not found
	return &models.NotificationPreferences{
		UserID:          userID,
		EmailEnabled:    false,
		TelegramEnabled: false,
	}
}

// GetUserWithPreferences returns preferences from cache by user ID
func (m *UserCacheManager) GetUserWithPreferences(userID int) *UserWithPreferences {
	m.dataMutex.RLock()
	defer m.dataMutex.RUnlock()

	if data, exists := m.userData[userID]; exists && data.Preferences != nil {
		// Return a copy to avoid race conditions
		prefsCopy := *data.Preferences
		return &UserWithPreferences{
			Preferences: &prefsCopy,
		}
	}

	// Return defaults if not found
	return &UserWithPreferences{
		Preferences: &models.NotificationPreferences{
			UserID:          userID,
			EmailEnabled:    false,
			TelegramEnabled: false,
		},
	}
}
