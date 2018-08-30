//To compile:
// g++ -O3 -I /cvmfs/cms-ib.cern.ch/nweek-02539/slc7_amd64_gcc810/external/tbb/2018_U1/include/ -DTBB_USE_GLIBCXX_VERSION=80101 -L/cvmfs/cms-ib.cern.ch/nweek-02539/slc7_amd64_gcc810/external/tbb/2018_U1/lib -ltbb -pthread -std=c++17 test.cc


#include <cmath>
#include <atomic>
#include <iostream>
#include <memory>
#include <cstdlib>
#include <cassert>
#include "tbb/task_scheduler_init.h"
#include "tbb/task.h"

static std::atomic<unsigned long> nModulesRun{0};
static std::atomic<unsigned long> nModulesStarted{0};

double numericIntegral() {
  //constexpr unsigned long long int kIterations = 1000000;
  constexpr unsigned long long int kIterations = 10000;
  constexpr double kStep = M_PI/kIterations/2;

  double result;
  for(unsigned long long int i=0; i< kIterations; ++i) {
    result += sin(i*kStep)*kStep;
  }
  ++nModulesRun;
  return result;

}

template<typename F>
class FunctorTask : public tbb::task {
public:
  FunctorTask( F iF) : m_f(std::move(iF) ) {}

  tbb::task* execute() override {
    m_f();
    return nullptr;
  };
private:
  F m_f;
};


template< typename F>
FunctorTask<F>* make_functor_task( F&& f) {
  return new (tbb::task::allocate_root()) FunctorTask<F>(std::move(f));
}

namespace waitingtask {
  struct TaskDestroyer {
    void operator()(tbb::task* iTask) const {
      tbb::task::destroy(*iTask);
    }
  };
}

class EmptyWaitingTask : public tbb::task {
public:
  EmptyWaitingTask() = default;
  
  tbb::task* execute() override { return nullptr;}
};

///Create an EmptyWaitingTask which will properly be destroyed
inline std::unique_ptr<EmptyWaitingTask, waitingtask::TaskDestroyer> make_empty_waiting_task() {
  return std::unique_ptr<EmptyWaitingTask, waitingtask::TaskDestroyer>( new (tbb::task::allocate_root()) EmptyWaitingTask{});
}


class WaitingTaskHolder {
public:
  WaitingTaskHolder(tbb::task* iTask):
    task_{iTask}
  {
    if(task_) { task_->increment_ref_count(); }
  }

  ~WaitingTaskHolder() {
    doneWaiting();
  }

  WaitingTaskHolder() = default;
  WaitingTaskHolder(WaitingTaskHolder const& iOther):
    task_{iOther.task_}
  {
    if(task_) {
      task_->increment_ref_count();
    }
  }
  WaitingTaskHolder(WaitingTaskHolder&& iOther):
    task_{iOther.task_}
  {
    iOther.task_ = nullptr;
  }
  WaitingTaskHolder& operator=(WaitingTaskHolder const& iRHS ) {
    WaitingTaskHolder temp(iRHS);
    std::swap(temp.task_,task_);
    return *this;
  }
  WaitingTaskHolder& operator=(WaitingTaskHolder&& iRHS) {
    WaitingTaskHolder temp(std::move(iRHS));
    std::swap(temp.task_,task_);
    return *this;
  }
    

  void doneWaiting() { 
    if(task_) {
      auto c =task_->decrement_ref_count(); 
      if(0 == c) {
        tbb::task::spawn(*task_);
      }
      task_=nullptr;
    }
  }
private:
  tbb::task* task_ = nullptr;
};

/* fails to compile
template< typename F>
void spawn_task(F&& iF) {
#pragma omp task untied firstprivate(iF)
  {
    iF();
  }
}
*/

template< typename F>
void spawn_task(F&& iF) {
  tbb::task::spawn(*make_functor_task(std::move(iF)) );
}


void run_module_async(WaitingTaskHolder h, unsigned long iEventID) {
  ++nModulesStarted;
  spawn_task([holder = std::move(h), iEventID] () mutable {
      auto v = numericIntegral();
      if(v<0.99) {
        std::cout <<"BAD"<<std::endl;
      } else {
        //std::cout <<v<<std::endl;
      }
      holder.doneWaiting();
    });
}


void process_event(unsigned int iStreamID, unsigned long iEventID, WaitingTaskHolder h) {

  WaitingTaskHolder finishedEventTask {
    make_functor_task([holder=std::move(h), iEventID, iStreamID]() mutable {
        //std::cout <<"finished "<<iStreamID<<std::endl;
        holder.doneWaiting();
      })};
  for(unsigned int i=0; i< 5; ++i) {
    run_module_async(finishedEventTask, iEventID);
  }
}

void get_and_process_events(unsigned int iStreamID, std::atomic<unsigned long>& nEventsProcessed, WaitingTaskHolder doneWithEvents) {
  //constexpr unsigned long kMaxEvents = 1000;
  constexpr unsigned long kMaxEvents = 100000;
  constexpr unsigned int kPrintEvent = kMaxEvents/10;
  auto v = ++nEventsProcessed;
  if(v< kMaxEvents+1) {
    if( 0 == (v %kPrintEvent) ) {
      std::cout <<v<<" "<<nModulesRun.load()<<" "<<nModulesStarted.load()<<std::endl;
    }
    spawn_task( [iStreamID, &nEventsProcessed, v, h =std::move(doneWithEvents)]()
    {
      process_event(iStreamID, v,
                    make_functor_task([iStreamID,&nEventsProcessed, dwe=std::move(h)]() { get_and_process_events(iStreamID,nEventsProcessed,std::move(dwe)); }) );
    });
  } else {
    doneWithEvents.doneWaiting();
  }
}

int main(int argc, char* argv[]) {

  std::atomic<unsigned long> nEventsProcessed{0};

  assert(argc == 3);

  const int nStreams = std::atoi(argv[1]);
  const unsigned int nThreads = std::atoi(argv[2]);
  
  std::cout <<"streams "<<nStreams<<" threads "<<nThreads<<std::endl;

  tbb::task_scheduler_init sched(nThreads);
  {
    auto waitTask = make_empty_waiting_task();
    waitTask->increment_ref_count();
    {
      WaitingTaskHolder h{waitTask.get()};
        for(unsigned int i=0; i<nStreams;++i) {
          spawn_task([i,&nEventsProcessed,h]()
                     {
                       get_and_process_events(i, nEventsProcessed, std::move(h));
                     });
        }
    }
    waitTask->wait_for_all();
  }

  std::cout <<"modules run "<<nModulesRun.load()<<std::endl;
  return 0;
}
