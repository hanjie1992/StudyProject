import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * @author: hanj
 * @date: 2019/7/17
 * @description: java集合 按照不同的列排序
 */
public class ComplexSorting {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat format= new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        Date date1=format.parse("20190713 12:00:00");
        Date date2=format.parse("20190713 12:01:00");
        Date date3=format.parse("20190713 12:01:00");
        Date date4=format.parse("20190713 12:03:00");
        Date date5=format.parse("20190713 12:04:00");
        List<Student> list = new ArrayList();
        Student student1 = new Student();
        student1.setTime(date1);
        student1.setScore(50);
        list.add(student1);
        Student student2 = new Student();
        student2.setTime(date2);
        student2.setScore(52);
        list.add(student2);
        Student student3 = new Student();
        student3.setTime(date3);
        student3.setScore(51);
        list.add(student3);
        Student student4 = new Student();
        student4.setTime(date4);
        student4.setScore(55);
        list.add(student4);
        Student student5 = new Student();
        student5.setTime(date5);
        student5.setScore(60);
        list.add(student5);
        for (Student li:list){
            System.out.println(li.toString());
        }
        System.out.println("========");
        list.sort(new Td());
        for (Student li:list){
            System.out.println(li.toString());
        }
        System.out.println("========");
        list.sort(new Tr());
        for (Student li:list){
            System.out.println(li.toString());
        }
        System.out.println("========");
        Comparator<Student> byTime = Comparator.comparing(Student::getTime);
        Comparator<Student> byScore = Comparator.comparing(Student::getScore);
        list.sort(byTime.thenComparing(byScore));
        for (Student li:list){
            System.out.println(li.toString());
        }
    }
}
class Td implements Comparator<Student> {
    @Override
    public int compare(Student o1, Student o2) {
        int cResult = 0;
        long a = o1.getTime().getTime() - o2.getTime().getTime();
        if (a != 0){
            cResult = (a > 0) ? 2 : -1;
        }else{
            a = o1.getScore() - o2.getScore();
            if (a != 0) {
                cResult = (a > 0) ? 1 : -1;
            }
        }
        return cResult;
    }
}
class Tr implements Comparator<Student> {
    @Override
    public int compare(Student o1, Student o2) {
        int result = o1.getTime().compareTo(o2.getTime());
        if (result == 0) {
            result = o1.getScore() - o2.getScore();
        }
        return result;
    }
}
class Student{
    Date time;
    int score;

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Student{" +
                "time=" + time +
                ", score=" + score +
                '}';
    }
}