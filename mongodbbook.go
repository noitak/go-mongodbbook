package main

import (
	"errors"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

func main() {
	session, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Create Unicorn collection
	unicorn, err := insertUnicorns(session)
	if err != nil {
		panic(err)
	}
	if err := ch01(unicorn); err != nil {
		panic(err)
	}
	if err := ch02(unicorn); err != nil {
		panic(err)
	}
	if err := ch02Hits(session); err != nil {
		panic(err)
	}
	if err := ch03(unicorn); err != nil {
		panic(err)
	}
	if err := ch04(session); err != nil {
		panic(err)
	}
	// Cleanup
	if err := cleanupCollection(session, "unicorn"); err != nil {
		panic(err)
	}
	if err := cleanupCollection(session, "hits"); err != nil {
		panic(err)
	}
	if err := cleanupCollection(session, "employees"); err != nil {
		panic(err)
	}
}

type Unicorn struct {
	ID         bson.ObjectId `bson:"_id,omitempty"`
	Name       string
	Dob        time.Time
	Loves      []string
	Weight     int
	Gender     string
	Vampires   int
	Vaccinated bool
}

type UnicornWithoutVampires struct {
	ID     bson.ObjectId `bson:"_id,omitempty"`
	Name   string
	Dob    time.Time
	Loves  []string
	Weight int
	Gender string
	//	Vampires int // comment out
	Vaccinated bool
}

func insertUnicorns(s *mgo.Session) (*mgo.Collection, error) {
	c := s.DB("test").C("unicorn")
	err := c.Insert(
		&Unicorn{Name: "Horny", Dob: time.Date(1992, 2, 13, 7, 47, 0, 0, time.Local), Loves: []string{"carrot", "papaya"}, Weight: 600, Gender: "m", Vampires: 63},
		&Unicorn{Name: "Aurora", Dob: time.Date(1991, 0, 24, 13, 0, 0, 0, time.Local), Loves: []string{"carrot", "grape"}, Weight: 450, Gender: "f", Vampires: 43},
		&Unicorn{Name: "Unicrom", Dob: time.Date(1973, 1, 9, 22, 10, 0, 0, time.Local), Loves: []string{"energon", "redbull"}, Weight: 984, Gender: "m", Vampires: 182},
		&Unicorn{Name: "Roooooodles", Dob: time.Date(1979, 7, 18, 18, 40, 0, 0, time.Local), Loves: []string{"apple"}, Weight: 575, Gender: "m", Vampires: 99},
		&Unicorn{Name: "Solnara", Dob: time.Date(1985, 6, 4, 2, 10, 0, 0, time.Local), Loves: []string{"apple", "carrot", "chocolate"}, Weight: 550, Gender: "f", Vampires: 80},
		&Unicorn{Name: "Ayna", Dob: time.Date(1998, 2, 7, 8, 30, 0, 0, time.Local), Loves: []string{"strawberry", "lemon"}, Weight: 733, Gender: "f", Vampires: 40},
		&Unicorn{Name: "Kenny", Dob: time.Date(1997, 6, 1, 10, 420, 0, 0, time.Local), Loves: []string{"grape", "lemon"}, Weight: 690, Gender: "m", Vampires: 39},
		&Unicorn{Name: "Raleigh", Dob: time.Date(205, 4, 3, 0, 570, 0, 0, time.Local), Loves: []string{"apple", "sugar"}, Weight: 421, Gender: "m", Vampires: 2},
		&Unicorn{Name: "Leia", Dob: time.Date(201, 9, 8, 14, 530, 0, 0, time.Local), Loves: []string{"apple", "watermelon"}, Weight: 601, Gender: "f", Vampires: 33},
		&Unicorn{Name: "Pilot", Dob: time.Date(1997, 2, 1, 5, 30, 0, 0, time.Local), Loves: []string{"apple", "watermelon"}, Weight: 650, Gender: "m", Vampires: 54},
		//		&Unicorn{Name: "Nimue", Dob: time.Date(1999, 11, 20, 16, 150, 0, 0, time.Local), Loves: []string{"grape", "carrot"}, Weight: 540, Gender: "f"},
		&Unicorn{Name: "Dunx", Dob: time.Date(1976, 6, 18, 18, 180, 0, 0, time.Local), Loves: []string{"grape", "watermelon"}, Weight: 704, Gender: "m", Vampires: 165})
	if err != nil {
		return nil, err
	}
	if err := c.Insert(&UnicornWithoutVampires{Name: "Nimue", Dob: time.Date(1999, 11, 20, 16, 150, 0, 0, time.Local), Loves: []string{"grape", "carrot"}, Weight: 540, Gender: "f"}); err != nil {
		return nil, err
	}
	return c, nil
}

func cleanupCollection(s *mgo.Session, collectionName string) error {
	c := s.DB("test").C(collectionName)
	if c == nil {
		return errors.New("can't create collection: " + collectionName)
	}
	_, err := c.RemoveAll(nil)
	if err != nil {
		return err
	}
	return nil
}

func ch01(c *mgo.Collection) error {
	fmt.Println("Ch01")

	//
	fmt.Println("性別が男で体重が700ポンドより大きいユニコーンを探す")
	var unicorns []Unicorn
	err := c.Find(
		bson.M{"gender": "m",
			"weight": bson.M{"$gt": 700}}).All(&unicorns)
	if err != nil {
		return err
	}
	for _, u := range unicorns {
		fmt.Printf("%v\n", u)
	}

	//
	fmt.Println("$exists演算子はフィールドの存在や欠如のマッチに利用します")
	var unicornsWithoutVampires []Unicorn
	err = c.Find(bson.M{"vampires": bson.M{"$exists": false}}).All(&unicornsWithoutVampires)
	if err != nil {
		return err
	}
	for _, u := range unicornsWithoutVampires {
		fmt.Printf("%v: %d\n", u, u.Vampires)
	}

	//
	fmt.Println("全ての女性のユニコーンの中から、りんごかオレンジが好き、もしくは体重が 500ポンド未満の条件で検索します")
	err = c.Find(
		bson.M{"gender": "f",
			"$or": []bson.M{
				bson.M{"loves": "apple"},
				bson.M{"loves": "orange"},
				bson.M{"weight": bson.M{"$lt": 500}}}}).All(&unicorns)
	if err != nil {
		return err
	}
	for _, u := range unicorns {
		fmt.Printf("%v\n", u)
	}
	return nil
}

func ch02(c *mgo.Collection) error {
	fmt.Println("Ch02")

	//
	fmt.Println("Roooooodles の体重を少し増やしたい")
	// before
	if err := printUnicorn(c, "Roooooodles"); err != nil {
		return err
	}
	if err := c.Update(bson.M{"name": "Roooooodles"}, bson.M{"$set": bson.M{"weight": 590}}); err != nil {
		fmt.Println("Fail to update Roooooodles")
		return err
	}
	// after
	if err := printUnicorn(c, "Roooooodles"); err != nil {
		return err
	}

	//
	fmt.Println("もしPilotがvampireを倒した数が間違っていて2つ多かった場合、以下のようにして間違いを修正します")
	// before
	if err := printUnicorn(c, "Pilot"); err != nil {
		return err
	}
	if err := c.Update(bson.M{"name": "Pilot"}, bson.M{"$inc": bson.M{"vampires": -2}}); err != nil {
		fmt.Println("Fail to update Pilot")
		return err
	}
	// after
	if err := printUnicorn(c, "Pilot"); err != nil {
		return err
	}

	//
	fmt.Println("もし Aurora が突然甘党になったら")
	// before
	if err := printUnicorn(c, "Aurora"); err != nil {
		return err
	}
	if err := c.Update(bson.M{"name": "Aurora"}, bson.M{"$push": bson.M{"loves": "sugar"}}); err != nil {
		return err
	}
	// after
	if err := printUnicorn(c, "Aurora"); err != nil {
		return err
	}

	//
	fmt.Println("全てのかわいいユニコーン達が予防接種を受けた")

	fmt.Println("before")
	var unicorns []Unicorn
	if err := c.Find(nil).All(&unicorns); err != nil {
		return err
	}
	for _, u := range unicorns {
		fmt.Printf("%s\tvaccinated:%t\n", u.Name, u.Vaccinated)
	}

	if _, err := c.UpdateAll(nil, bson.M{"$set": bson.M{"vaccinated": true}}); err != nil {
		return err
	}
	fmt.Println("after")
	if err := c.Find(nil).All(&unicorns); err != nil {
		return err
	}
	for _, u := range unicorns {
		fmt.Printf("%s\tvaccinated:%t\n", u.Name, u.Vaccinated)
	}

	return nil
}

type Hits struct {
	ID   bson.ObjectId `bson:"_id,omitempty"`
	Page string
	Hits int
}

func ch02Hits(s *mgo.Session) error {
	//
	fmt.Println("Webサイトのカウンター")
	c := s.DB("test").C("hits")
	// 1
	if _, err := c.Upsert(bson.M{"page": "unicorns"}, bson.M{"$inc": bson.M{"hits": 1}}); err != nil {
		return err
	}
	var h Hits
	if err := c.Find(bson.M{"page": "unicorns"}).One(&h); err != nil {
		return err
	}
	fmt.Printf("%v\n", h)
	// 2
	if _, err := c.Upsert(bson.M{"page": "unicorns"}, bson.M{"$inc": bson.M{"hits": 1}}); err != nil {
		return err
	}
	if err := c.Find(bson.M{"page": "unicorns"}).One(&h); err != nil {
		return err
	}
	fmt.Printf("%v\n", h)

	return nil
}

func ch03(c *mgo.Collection) error {
	fmt.Println("Ch03")

	//

	//
	fmt.Println("全てのユニコーンの名前を取得")
	var names []Unicorn
	c.Find(nil).Select(bson.M{"name": 1}).All(&names)
	for _, n := range names {
		fmt.Printf("%v\n", n)
	}

	//
	fmt.Println("昇順でソートを行いたい場合はフィールドと1を指")
	var us []Unicorn
	c.Find(nil).Sort("name").All(&us)
	for _, u := range us {
		fmt.Printf("%v\n", u.Name)
	}

	fmt.Println("降順で行いたい場合は–1を指定します")
	c.Find(nil).Sort("-name").All(&us)
	for _, u := range us {
		fmt.Printf("%v\n", u.Name)
	}

	//
	fmt.Println("ページング:2番目と3番目に重いユニコーンを得る")
	c.Find(nil).Sort("-weight").Limit(2).Skip(1).All(&us)
	for _, u := range us {
		fmt.Printf("%s(weight: %d)\n", u.Name, u.Weight)
	}

	//
	fmt.Println("カウント")
	n, err := c.Find(bson.M{"vampires": bson.M{"$gt": 50}}).Count()
	if err != nil {
		return err
	}
	fmt.Println(n)

	return nil
}

func printUnicorn(c *mgo.Collection, name string) error {
	var u Unicorn
	err := c.Find(bson.M{"name": name}).One(&u)
	if err != nil {
		fmt.Println("Fail to find: ", name)
		return err
	}
	fmt.Printf("%v\n", u)
	return nil
}

type Employee struct {
	ID   bson.ObjectId `bson:"_id,omitempty"`
	Name string
}

type EmployeeHasManager struct {
	ID      bson.ObjectId `bson:"_id,omitempty"`
	Name    string
	Manager bson.ObjectId
}

type EmployeeHasMultiManager struct {
	ID       bson.ObjectId `bson:"_id,omitempty"`
	Name     string
	Managers []bson.ObjectId
}

func ch04(s *mgo.Session) error {
	fmt.Printf("\nCh04\n")

	c := s.DB("test").C("employees")
	if c == nil {
		return errors.New("Can't create employees collection")
	}
	if err := c.Insert(&Employee{Name: "Maho"}); err != nil {
		return err
	}
	// MahoのID
	var maho Employee
	c.Find(bson.M{"name": "Maho"}).One(&maho)

	if err := c.Insert(
		&EmployeeHasManager{Name: "Miho", Manager: maho.ID},
		&EmployeeHasManager{Name: "Erika", Manager: maho.ID}); err != nil {
		return err
	}

	//
	fmt.Printf("Mihoの社員を検索する\n")
	var es []EmployeeHasManager
	c.Find(bson.M{"manager": maho.ID}).All(&es)
	for _, e := range es {
		fmt.Printf("%s : manager=%s\n", e.Name, e.Manager)
	}

	//
	fmt.Printf("\n社員が複数のマネージャーを持つ場合\n")
	var miho Employee
	c.Find(bson.M{"name": "Miho"}).One(&miho)

	if err := c.Insert(&EmployeeHasMultiManager{Name: "Koume", Managers: []bson.ObjectId{maho.ID, miho.ID}}); err != nil {
		return err
	}
	var ems []EmployeeHasMultiManager
	c.Find(bson.M{"managers": miho.ID}).All(&ems)
	for _, e := range ems {
		fmt.Printf("%s : manager=%v\n", e.Name, e.Managers)
	}

	// familyの例は型が面倒なので割愛

	//
	fmt.Printf("\nDBRef\n")
	s.FindRef(&mgo.DBRef{Collection: "employees", Id: miho.ID, Database: "test"}).All(&es)
	for _, e := range es {
		fmt.Printf("%v -> %v\n", miho.ID, e.Name)
	}

	return nil
}
