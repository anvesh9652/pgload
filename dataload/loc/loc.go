package loc

import "time"

func GetTimeForLocation(timeZone string) (string, error) {
	loc, err := time.LoadLocation(timeZone)
	if err != nil {
		return "", err
	}
	t := time.Now().In(loc)
	str := t.Format(time.DateTime)
	return str, nil
}
