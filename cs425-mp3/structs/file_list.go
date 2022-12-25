package fl

import (
	"log"
	"sort"
	"strconv"
	"strings"
)

type FileList struct {
	FileToAdr                   map[string]map[string]uint
	AdrToFile                   map[string]map[string]uint
	NameVersionToRemoteFileName map[string]map[string]string
	RemoteFileNameToNameVersion map[string]string
}

type CompareHelperStruct struct {
	Alias        string
	RealFileName string
}

func InitFileList() FileList {
	return FileList{FileToAdr: make(map[string]map[string]uint),
		AdrToFile:                   make(map[string]map[string]uint),
		NameVersionToRemoteFileName: make(map[string]map[string]string),
		RemoteFileNameToNameVersion: make(map[string]string)} // delete allversion doesn't update this map!
}

// len fileNames == versions (version of each file added), add those to each adr in addresses
func (f *FileList) Add(fileNames []string, addresses []string, versions []uint) {
	for _, adr := range addresses { // updating AdrToFile
		if _, exist := f.AdrToFile[adr]; exist {
			for versionIdx, fN := range fileNames {
				f.AdrToFile[adr][fN] = versions[versionIdx]
			}
		} else {
			newList := make(map[string]uint)
			for versionIdx, fN := range fileNames {
				newList[fN] = versions[versionIdx]
			}
			f.AdrToFile[adr] = newList
		}
	}

	for versionIdx, fN := range fileNames { // updating File to Adr
		if _, exist := f.FileToAdr[fN]; exist {
			for _, adr := range addresses {
				f.FileToAdr[fN][adr] = versions[versionIdx]
			}
		} else {
			newList := make(map[string]uint)
			for _, adr := range addresses {
				newList[adr] = versions[versionIdx]
			}
			f.FileToAdr[fN] = newList
		}
	}
}

// return array of addresses of replicas needs to be deleted
func (f *FileList) DeleteFile(fileName string) []string {
	var ret []string
	for adr, _ := range f.FileToAdr[fileName] {
		delete(f.AdrToFile[adr], fileName)
		ret = append(ret, adr)
	}
	delete(f.FileToAdr, fileName)
	return ret
}

// return an array of file names that needs to be reallocated
func (f *FileList) DeleteNode(address string) ([]string, []uint) {
	var fileNames []string
	var fileIds []uint
	for fN, Id := range f.AdrToFile[address] {
		fileNames = append(fileNames, fN)
		fileIds = append(fileIds, Id)
		delete(f.FileToAdr[fN], address)
		delete(f.AdrToFile[address], fN)
	}
	delete(f.AdrToFile, address)

	return fileNames, fileIds
}

// return addresses of all replicas with the latest version
func (f *FileList) GetFile(fileName string) []string {
	var addresses []string
	var latestVersion uint = 0
	for _, version := range f.FileToAdr[fileName] {
		if version > latestVersion {
			latestVersion = version
		}
	}
	for address, version := range f.FileToAdr[fileName] {
		if version == latestVersion {
			addresses = append(addresses, address)
		}
	}
	return addresses
}

// return list of address for a given file
func (f *FileList) ShowReplicas(fileName string) []string {
	var addresses []string
	for adr, _ := range f.FileToAdr[fileName] {
		addresses = append(addresses, adr)
	}
	return addresses
}

func (f *FileList) AddTranslation(fileName string, alias string, fileId uint) bool {
	newFileName := strconv.FormatUint(uint64(fileId), 10) + " " + fileName
	if val1, exist1 := f.NameVersionToRemoteFileName[fileName]; exist1 {
		if _, exist2 := val1[alias]; exist2 {
			log.Printf("AddTranslation: Overwriting existing translation: %s + %s\n", fileName, alias)
			return false
		}
		val1[alias] = newFileName
	} else {
		newMap := make(map[string]string)
		newMap[alias] = newFileName
		f.NameVersionToRemoteFileName[fileName] = newMap
	}
	f.RemoteFileNameToNameVersion[newFileName] = fileName + "  version:" + alias
	return true
}

func (f *FileList) Translate(fileName string, alias string) string {
	if val1, exist1 := f.NameVersionToRemoteFileName[fileName]; exist1 {
		if val2, exist2 := val1[alias]; exist2 {
			return val2
		} else {
			log.Printf("Translate: can't find fileVersion: %s\n", alias)
			return ""
		}
	} else {
		log.Printf("Translate: can't find FileName: %s\n", fileName)
		return ""
	}
}

// delete a spcific version of a file translation
func (f *FileList) DeleteTranslationOneVersion(fileName string, alias string) {
	if val1, exist1 := f.NameVersionToRemoteFileName[fileName]; exist1 {
		if remoteName, exist2 := val1[alias]; exist2 {
			delete(f.RemoteFileNameToNameVersion, remoteName)
			delete(val1, alias)

		} else {
			log.Printf("DeleteTranslate: can't find fileVersion to delete: %s\n", alias)
		}
	} else {
		log.Printf("DeleteTranslate: can't find FileName to delete: %s\n", fileName)
	}
}

// delete all version of a file translation
func (f *FileList) DeleteTranslationAllVersion(fileName string) {
	if val1, exist1 := f.NameVersionToRemoteFileName[fileName]; exist1 {
		for _, remoteName := range val1 {
			delete(f.RemoteFileNameToNameVersion, remoteName)
		}
		delete(f.NameVersionToRemoteFileName, fileName)
	} else {
		log.Printf("DeleteTranslate: can't find FileName to delete: %s\n", fileName)
	}
}

func (f *FileList) IncrementSortedCompHelper(fileName string) ([]CompareHelperStruct, bool) {
	var versions []CompareHelperStruct
	if val, exist := f.NameVersionToRemoteFileName[fileName]; exist {
		for alias, realName := range val {
			versions = append(versions, CompareHelperStruct{Alias: alias, RealFileName: realName})
		}
		sort.Slice(versions, func(i, j int) bool { // return true if a < b
			a := versions[i]
			b := versions[j]
			if len(a.RealFileName) != len(b.RealFileName) {
				if len(a.RealFileName) < len(b.RealFileName) { // find new latest version
					return true
				}
				return false
			} else {
				if strings.Compare(a.RealFileName, b.RealFileName) == 1 { // same #digits, b>a ID
					return false
				}
				return true
			}
		})
		return versions, true

	} else {
		log.Printf("IncrementSortedCompHelper: can't find FileName : %s\n", fileName)
		return versions, false
	}
}

// return false if fileName doesn't exist
func (f *FileList) GetAllVersions(fileName string) ([]string, bool) {
	var latestVersions []string
	versions, success := f.IncrementSortedCompHelper(fileName)
	if !success {
		log.Printf("GetAllVersions: IncrementSortedCompHelper failed")
		return latestVersions, false
	}

	for i := len(versions) - 1; i >= 0; i-- {
		latestVersions = append(latestVersions, versions[i].Alias)
	}
	return latestVersions, true
}

// return false if fileName doesn't exist
func (f *FileList) GetLatestVersions(fileName string, howMany int) ([]string, bool) {
	var latestVersions []string
	versions, success := f.IncrementSortedCompHelper(fileName)
	if !success {
		log.Printf("GeLatestVersions: IncrementSortedCompHelper failed")
		return latestVersions, false
	}

	if howMany > len(versions) {
		log.Printf("GetLatestVersions: don't have enough version: %d need %d\n", len(versions), howMany)
		return latestVersions, false
	}
	for i := len(versions) - 1; i > len(versions)-1-howMany; i-- {
		latestVersions = append(latestVersions, versions[i].Alias)
	}
	return latestVersions, true
}

func (f *FileList) ShowFileInAddress(address string) []string {
	var localNames []string
	for name, _ := range f.AdrToFile[address] {
		localNames = append(localNames, f.RemoteFileNameToNameVersion[name])
	}
	return localNames
}
