package study.querydsl;

import com.querydsl.core.Tuple;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.*;

@SpringBootTest
@Transactional
@Commit
public class QuerydslBasicTest {
    @Autowired
    EntityManager em;
    JPAQueryFactory queryFactory;

    @BeforeEach // 테스트 실행 전 데이터 넣기 위함
    public void befor() {
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");


        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);


        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        String qlString = "select m from Member m where m.username = :username";
        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void startQuerydsl() {
        Member member1 = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1")) // JPQL과 다르게 파라미터 바인딩 처리가 자동으로 됨
                .fetchOne();

        assertThat(member1.getUsername()).isEqualTo("member1");
    }

    @Test
    void search() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.between(10, 20))
                        .and(member.age.notIn(15)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void searchAndParam() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void resultFetch() {
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();

        Member fetchOne = queryFactory
                .selectFrom(member)
                .fetchOne();

        Member fetchFirs = queryFactory
                .selectFrom(member)
                .fetchFirst();// limit 1
    }


    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순
     * 2. 회원 이름 올림차순
     * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
     */
    @Test
    void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast()) // null이면 마지막으로 정렬
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }


    @Test
    public void paging1() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) // 몇개를 스킵할지, 0부터 시작
                .limit(2)
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    // Tuple : select 타입이 2개 이상일 경우 반환값은 Tuple임
    @Test
    public void aggregation() {
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령
     */
    @Test
    void group() {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team) // 조인 대상, alias
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     * 팀 A에 소속된 모든 회원
     */
    @Test
    public void join() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).getUsername()).isEqualTo("member1");
        assertThat(result.get(1).getUsername()).isEqualTo("member2");

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");

    }


    /**
     * 세타 조인(연관 관계 없는 테이블을 조인, from에 테이블 이름을 같이 써줌)
     * 회원 이름이 팀 이름과 같은 회원 조회
     * 모든 멤버 테이블과 모든 팀 테이블을 조인해서 조건에 맞는 데이터만 select
     * outer 조인은 불가능 (양 쪽 테이블에서 완전 겹치는 것만 가져옴, 같지 않는 데이터는 null로 가져오면 안됨)
     */
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name)) // 이게돼? 와우..
                .fetch();

        for (Member member : result) {
            System.out.println("member = " + member);
        }

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }


    /**
     * 조인 on
     * 조인 필터링
     * 연관관계 없는 엔티티 외부 조인
     * 회원과 팀을 조인하면서 팀 이름이 teamA인 팀만 조인하고 회원은 모든 조회해라
     */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
/*                member.team 을 지우게 되면 각 member에 매핑된 team의 이름이 아니라 전체 team 중에서 teamA인 것을
                매번 조회해서 List에 넣게됨. 따라서 TeamA가 두개더라도, Member의 수만큼 TeamA가 조회됨(4번)*/
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
//                .where(team.name.eq("teamA"))
                .fetch();


        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

        /* 참고: on 절을 활용해 조인 대상을 필터링 할 때, 외부조인이 아니라 내부조인(inner join)을 사용하면,
        where 절에서 필터링 하는 것과 기능이 동일하다. 따라서 on 절을 활용한 조인 대상 필터링을 사용할 때,
                내부조인 이면 익숙한 where 절로 해결하고, 정말 외부조인이 필요한 경우에만 이 기능을 사용하자.*/
    }

    /**
     * 연관관계가 없는 엔티티 외부 조인
     * 회원의 이름이 팀 이름과 같은 대상을 외부 조인(같지 않는 데이터는 null로 가져와야함) -> theta 조인 한계 극복
     */
    @Test
    public void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                /* member.team을 넣으면 member1~4는 team과 member 이름이 다르고,
                teamA,B 이름의 멤버는 팀이 없으므로 모든 팀이 null로 보임  */
                /* member.team을 빼면 각 member를 돌면서 전체 team 중 같은 이름의 team이 보일때마다
                 * 해당 차례의 member에서 그 team을 가져옴. 그래서 마지막 2번에만 찍힘 */
                .leftJoin(team)
                .on(member.username.eq(team.name)) // 내부조인의 경우는 where로 대체 가능
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }
    /*
     * 정리: 연관관계가 없는 엔티티를 on을 사용해서 outer 조인 할 때는
     * leftjoin에 (member.team, team)이 아니라, (team)만 적는 식으로 사용해야함!!
     * */

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        // 로딩됭 엔티티인지 아닌지 검증해줌
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isEqualTo(false); // 지연로딩이므로 team 엔티티를 가져오지 않은 상태여야함
    }

    @Test
    public void fetchJoinUse() {
        em.flush();
        em.clear();

        // 페치조인으로 연관된 엔티티를 한번에 select 한다.
        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        // 로딩됭 엔티티인지 아닌지 검증해줌
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 적용").isEqualTo(true); // 지연로딩이므로 team 엔티티를 가져오지 않은 상태여야함
    }

    /**
     * 나이가 가장 많은 회원 조회
     * 서브쿼리문: JPAExpressions 사용
     */
    @Test
    public void subQuery() {

        // 서브쿼리문 생성 시에는 앨리어스가 겹치면 안되기 때문에 Q객체를 생성해줘야함
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();


        assertThat(result).extracting("age").containsExactly(40);
    }

    /**
     * 나이가 평균 이상인 회원
     */
    @Test
    public void subQueryGoe() {

        // 서브쿼리문 생성 시에는 앨리어스가 겹치면 안되기 때문에 Q객체를 생성해줘야함
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();


        assertThat(result).extracting("age").containsExactly(30,40);
    }

    @Test
    public void subQueryIn() {

        // 서브쿼리문 생성 시에는 앨리어스가 겹치면 안되기 때문에 Q객체를 생성해줘야함
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        List<Member> result1 = queryFactory
                .selectFrom(member)
                .where(member.age.gt(10))
                .fetch();


        assertThat(result).extracting("age").containsExactly(20, 30,40);
        assertThat(result1).extracting("age").containsExactly(20, 30,40);
    }


    @Test
    public void selectSubQuery() {
        // 서브쿼리문 생성 시에는 앨리어스가 겹치면 안되기 때문에 Q객체를 생성해줘야함
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        select(memberSub.age.avg()) // JPAExpressions static 처리
                                .from(memberSub))
                .from(member)
                .where(member.age.eq(10))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.username)).isEqualTo("member1");

    }

    /**
     * from 절의 서브쿼리 해결방안
     * 1. 서브쿼리를 join으로 변경한다. (가능한 상황도 있고, 불가능한 상황도 있다.)
     * 2. 애플리케이션에서 쿼리를 2번 분리해서 실행한다.
     * 3. nativeSQL을 사용한다.
     */

}
